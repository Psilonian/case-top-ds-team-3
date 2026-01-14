import argparse
import csv
import datetime as dt
import os
import time
from collections import defaultdict

import requests

API_URL = "https://api.stackexchange.com/2.3/questions"


def to_unix(d: dt.datetime) -> int:
    # StackExchange ожидает UTC epoch seconds
    return int(d.astimezone(dt.timezone.utc).timestamp())


def month_delta(d: dt.datetime, months: int) -> dt.datetime:
    """Смещение по месяцам без внешних зависимостей."""
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    days_in_month = [
        31,
        29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ][m - 1]
    day = min(d.day, days_in_month)
    return d.replace(year=y, month=m, day=day)


def day_bucket(created_utc: dt.datetime) -> str:
    return created_utc.date().isoformat()  # YYYY-MM-DD


def fetch_questions(tag: str, fromdate: int, todate: int, pagesize: int = 100, max_pages: int = 50):
    """
    Генератор вопросов. Уважает backoff. Бросает RuntimeError на HTTP/API ошибках.
    """
    page = 1

    while page <= max_pages:
        params = {
            "site": "stackoverflow",
            "tagged": tag,
            "fromdate": fromdate,
            "todate": todate,
            "page": page,
            "pagesize": pagesize,
            "order": "asc",
            "sort": "creation",
            "filter": "default",
        }

        r = requests.get(API_URL, params=params, timeout=30)
        if r.status_code != 200:
            # В теле обычно error_name/error_message
            raise RuntimeError(f"HTTP {r.status_code} | tag={tag} | page={page} | body={r.text[:1500]}")

        payload = r.json()

        # лимиты/квота
        if "quota_remaining" in payload and payload["quota_remaining"] == 0:
            raise RuntimeError(f"Quota exhausted (quota_remaining=0) | tag={tag} | page={page}")

        # backoff от API (обязателен к соблюдению)
        backoff = int(payload.get("backoff", 0))
        if backoff > 0:
            time.sleep(backoff)

        items = payload.get("items", [])
        for item in items:
            yield item

        if not payload.get("has_more"):
            break

        page += 1


def main():
    p = argparse.ArgumentParser(description="Collect StackOverflow questions count by date and tag.")
    p.add_argument("--tags", nargs="+", required=True, help="Список тегов, например: python javascript java")
    p.add_argument("--out", default="data/questions_by_date.csv", help="Путь к выходному CSV")
    p.add_argument("--max-pages", type=int, default=50, help="Ограничение страниц на тег (pagesize*страниц)")
    p.add_argument("--months-from", type=int, default=6, help="Сколько месяцев назад старт (по умолчанию 6)")
    p.add_argument("--months-to", type=int, default=2, help="Сколько месяцев назад конец (по умолчанию 2)")
    args = p.parse_args()

    # Период: [now - months_from, now - months_to], включительно по датам.
    now = dt.datetime.now(dt.timezone.utc)

    start_dt = month_delta(now, -args.months_from).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = month_delta(now, -args.months_to).replace(hour=0, minute=0, second=0, microsecond=0)

    # Чтобы включить end_dt "как дату целиком", берём todate = end_dt + 1 день (00:00 следующего дня)
    # => получаем полуинтервал [start_dt, end_dt+1day)
    to_dt_exclusive = end_dt + dt.timedelta(days=1)

    fromdate = to_unix(start_dt)
    todate = to_unix(to_dt_exclusive)

    counts = defaultdict(int)  # (date, tag) -> count

    for tag in args.tags:
        print(f"TAG={tag} | period_utc: {start_dt.date()} .. {end_dt.date()} (inclusive)")

        last_created = None
        fetched = 0

        try:
            for q in fetch_questions(tag, fromdate, todate, max_pages=args.max_pages):
                created = dt.datetime.fromtimestamp(q["creation_date"], tz=dt.timezone.utc)
                counts[(day_bucket(created), tag)] += 1
                last_created = created
                fetched += 1
        except RuntimeError as e:
            print("ERROR:", e)
            print(f"Stopped: tag={tag} | fetched={fetched}")
            continue

        if last_created:
            print(f"Done: tag={tag} | fetched={fetched} | last_created_utc={last_created.isoformat()}")
        else:
            print(f"Done: tag={tag} | fetched=0")

    # Запись CSV
    rows = [{"date": d, "tag": tag, "questions_count": cnt} for (d, tag), cnt in counts.items()]
    rows.sort(key=lambda x: (x["date"], x["tag"]))

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date", "tag", "questions_count"])
        w.writeheader()
        w.writerows(rows)

    print(f"Saved: {args.out} | rows={len(rows)}")


if __name__ == "__main__":
    main()