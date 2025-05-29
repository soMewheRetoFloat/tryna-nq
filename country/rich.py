# postprocess_area_enwiki_views_with_tqdm.py

import json
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from datetime import datetime, timedelta
from tqdm import tqdm
headers = {
        "User-Agent": "YourAppName/1.0 (your_email@example.com)"
    }
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

def filter_area(area):
    if isinstance(area, (int, float)):
        if 0.1 < area < 1_000_000:
            return area
    elif area == "unknown":
        return area
    return None

def batch_get_enwiki_titles(qids):
    result = {}
    BATCH = 180
    total = len(qids)
    print("Querying enwiki titles from Wikidata...")
    for i in tqdm(range(0, total, BATCH), desc="enwiki title"):
        batch = qids[i:i+BATCH]
        values = " ".join(f"wd:{qid}" for qid in batch)
        query = f"""
        SELECT ?qid ?title WHERE {{
          VALUES ?qid {{ {values} }}
          OPTIONAL {{
            ?article schema:about ?qid;
                     schema:isPartOf <https://en.wikipedia.org/>;
                     schema:name ?title.
          }}
        }}
        """
        headers = {'User-Agent': 'enwiki-title-finder/1.0 (OpenAI user script)'}
        resp = requests.get(WIKIDATA_SPARQL, params={"query": query, "format": "json"}, headers=headers)
        items = resp.json()["results"]["bindings"]
        for row in items:
            qid = row["qid"]["value"].split("/")[-1]
            title = row.get("title", {}).get("value")
            if title:
                result[qid] = title.replace(' ', '_')
        time.sleep(0.5)
    return result

def collect_all_qids(data):
    qids = set()
    for country, cdata in data.items():
        if cdata.get("qid"): qids.add(cdata["qid"])
        for prov in cdata.get("subdivisions", []):
            if prov.get("qid"): qids.add(prov["qid"])
            for city in prov.get("children", []):
                if city.get("qid"): qids.add(city["qid"])
    return list(qids)

def assign_titles(data, qid2title):
    for country, cdata in data.items():
        qid = cdata.get("qid")
        if qid and qid2title.get(qid):
            cdata["title"] = qid2title[qid]
        for prov in cdata.get("subdivisions", []):
            qid = prov.get("qid")
            if qid and qid2title.get(qid):
                prov["title"] = qid2title[qid]
            for city in prov.get("children", []):
                qid = city.get("qid")
                if qid and qid2title.get(qid):
                    city["title"] = qid2title[qid]

def get_views_12m(title):
    now = datetime.utcnow()
    end = now.strftime('%Y%m%d')
    start = (now - timedelta(days=365)).strftime('%Y%m%d')
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{quote(title)}/monthly/{start}/{end}"
    try:
        resp = requests.get(url, timeout=10, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            return sum(item.get("views", 0) for item in data.get("items", []))
        else:
            return 0
    except Exception as e:
        return 0

def postprocess(input_path, output_path, max_workers=5):
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    def clean_entity_area(entity):
        area = entity.get("area_km2", None)
        new_area = filter_area(area)
        if new_area is None:
            entity.pop("area_km2", None)
        else:
            entity["area_km2"] = new_area

    # enwiki title
    qids = collect_all_qids(data)
    print(f"Total unique QIDs: {len(qids)}")
    qid2title = batch_get_enwiki_titles(qids)
    print(f"Entities with enwiki titles: {len(qid2title)}")
    assign_titles(data, qid2title)

    # pageviews
    query_tasks = []
    for cname, cdata in data.items():
        clean_entity_area(cdata)
        title = cdata.get("title", None)
        if title:
            query_tasks.append( (cdata, title, "views_12m") )
        for prov in cdata.get("subdivisions", []):
            clean_entity_area(prov)
            title = prov.get("title", None)
            if title:
                query_tasks.append( (prov, title, "views_12m") )
            for city in prov.get("children", []):
                clean_entity_area(city)
                # 可加城市pageviews

    def views_task(args):
        entity, title, field = args
        views = get_views_12m(title)
        entity[field] = views
        time.sleep(0.22)
        return title, views

    print("Querying pageviews for all entities...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        fut2label = {executor.submit(views_task, task): task[1] for task in query_tasks}
        for _ in tqdm(as_completed(fut2label), total=len(fut2label), desc="pageviews"):
            pass  # 只显示进度，不输出内容

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python postprocess_area_enwiki_views_with_tqdm.py input.json output.json [max_workers]", file=sys.stderr)
        sys.exit(1)
    src, dst = sys.argv[1], sys.argv[2]
    max_workers = int(sys.argv[3]) if len(sys.argv) == 4 else 5
    postprocess(src, dst, max_workers)
