import argparse
import csv
import datetime as dt
import time
from collections import defaultdict

import requests


API_URL = "https://api.stackexchange.com/2.3/questions"


def to_unix(d: dt.datetime) -> int:
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp())


def month_delta(d: dt.datetime, months: int) -> dt.datetime:
    # Простое смещение по месяцам без внешних зависимостей
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return d.replace(year=y, month=m, day=day)


def bucket_date(creation_utc: dt.datetime, grain: str) -> str:
    if grain == "day":
        return creation_utc.date().isoformat()
    # week: ISO week start (Monday)
    monday = creation_utc.date() - dt.timedelta(days=creation_utc.date().weekday())
    return monday.isoformat()


def fetch_questions(tag: str, fromdate: int, todate: int, pagesize: int = 100, max_pages: int = 50):
    page = 1
    backoff_total = 0

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

        # если ошибка — покажем тело ответа API и остановимся
        if r.status_code != 200:
          print("HTTP", r.status_code)
          print(r.text[:2000])   # тут будет error_name/error_message
          return

        payload = r.json()

        if "backoff" in payload:
            # уважение лимитов Stack Exchange
            b = int(payload["backoff"])
            backoff_total += b
            time.sleep(b)

        for item in payload.get("items", []):
            yield item

        if not payload.get("has_more"):
            break

        page += 1

    if backoff_total:
        pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tags", nargs="+", required=True, help="Список тегов, например: python pandas")
    p.add_argument("--grain", choices=["day", "week"], default="day", help="Агрегация: day или week")
    p.add_argument("--out", default="data/questions_count.csv", help="Путь к выходному CSV")
    p.add_argument("--max-pages", type=int, default=50, help="Ограничение страниц на тег (100*страниц вопросов)")
    args = p.parse_args()

    # Период: 2–6 месяцев назад от "сейчас" (UTC)
    now = dt.datetime.now(dt.timezone.utc)
    start = month_delta(now, -6)
    end = month_delta(now, -2)

    fromdate = to_unix(start)
    todate = to_unix(end)

    counts = defaultdict(int)  # (bucket_date, tag) -> count

    for tag in args.tags:
        for q in fetch_questions(tag, fromdate, todate, max_pages=args.max_pages):
            created = dt.datetime.fromtimestamp(q["creation_date"], tz=dt.timezone.utc)
            d = bucket_date(created, args.grain)
            counts[(d, tag)] += 1

    # Запись CSV: date, tag, questions_count
    rows = [
        {"date": d, "tag": tag, "questions_count": cnt}
        for (d, tag), cnt in counts.items()
    ]
    rows.sort(key=lambda x: (x["date"], x["tag"]))

    # ensure folder exists
    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date", "tag", "questions_count"])
        w.writeheader()
        w.writerows(rows)

    print(f"Saved: {args.out} | rows: {len(rows)} | period_utc: {start.date()}..{end.date()} | grain: {args.grain}")


if __name__ == "__main__":
    main()