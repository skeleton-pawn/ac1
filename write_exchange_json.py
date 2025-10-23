import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta, timezone
import os


def get_smbs_rates_xml(currency_code: str):
    """SMBS 환율 XML을 조회해서 날짜별 환율 리스트 반환"""
    today = datetime.now()
    end_date = today.strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")

    url = f"http://www.smbs.biz/ExRate/StdExRate_xml.jsp?arr_value={currency_code}_{start_date}_{end_date}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "xml")
    items = soup.find_all("set")

    records = []
    for item in items:
        label = item.get("label")
        value = item.get("value")
        if not label or not value:
            continue

        try:
            rate = float(value)
        except ValueError:
            continue

        records.append({
            "date": label.replace(".", "-"),
            "rate": rate
        })

    return records


def save_to_json(data, filename="exchange_rates.json"):
    existing = {}
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = {}

    merged = existing.copy()
    for cur, records in data.items():
        existing_dates = {r["date"] for r in merged.get(cur, [])}
        new_records = [r for r in records if r["date"] not in existing_dates]
        merged[cur] = sorted(merged.get(cur, []) + new_records, key=lambda x: x["date"])

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved to {filename} ({sum(len(v) for v in merged.values())} records total)")


def main():
    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST)
    print(f"[{now_kst:%Y-%m-%d %H:%M:%S}] 환율 데이터(XML) 수집 시작")

    data = {
        "USD": get_smbs_rates_xml("USD"),
        "JPY": get_smbs_rates_xml("JPY"),
    }

    save_to_json(data)


if __name__ == "__main__":
    main()
