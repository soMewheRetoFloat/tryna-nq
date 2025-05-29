# debug_pageviews_single.py

import requests
from urllib.parse import quote
from datetime import datetime, timedelta



def get_views_12m(title):
    now = datetime.utcnow()
    end = now.strftime('%Y%m%d')
    start = (now - timedelta(days=365)).strftime('%Y%m%d')
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{quote(title)}/monthly/{start}/{end}"
    headers = {
        "User-Agent": "YourAppName/1.0 (your_email@example.com)"
    }

    print(f"Querying: {url}")
    try:
        resp = requests.get(url, timeout=10, headers=headers)
        print(f"Status code: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            total_views = sum(item.get("views", 0) for item in data.get("items", []))
            print(f"Raw JSON (truncated): {json.dumps(data)[:400]} ...")
            print(f"Total views in 12 months: {total_views}")
            return total_views
        else:
            print(f"Response text (truncated): {resp.text[:400]}")
            return 0
    except Exception as e:
        print(f"Exception for {title}: {e}")
        return 0

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) != 2:
        print("Usage: python debug_pageviews_single.py <title>")
        sys.exit(1)
    title = sys.argv[1]
    get_views_12m(title)
