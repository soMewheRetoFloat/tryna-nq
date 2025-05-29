# fetch_award_bulk_by_subaward.py
# 一次查全每个子奖项所有获奖人及时间，并按年份聚合，避免限流

import json, time
from datetime import datetime
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

AWARD_FILE = "award_popularity.json"
FACTS_FILE = Path("structured_award_facts.json")
MAP_FILE = Path("award_sub_mapping.json")
WDQS = "https://query.wikidata.org/sparql"
TOP_K = 100
SLEEP = 1.5

sparql = SPARQLWrapper(WDQS, agent="PopPop/bulk-award-fetch 4.0")
sparql.setReturnFormat(JSON)

def run_query(q):
    for _ in range(3):
        try:
            sparql.setQuery(q)
            return sparql.query().convert()
        except Exception:
            time.sleep(2)
    return None

def get_sub_awards(qid):
    q = f"""
    SELECT DISTINCT ?sub ?subLabel WHERE {{
      {{ ?sub wdt:P361 wd:{qid}. }} UNION {{ wd:{qid} wdt:P527 ?sub. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}"""
    result = run_query(q)
    if not result: return []
    return [
        {"qid": b["sub"]["value"].split("/")[-1], "label": b["subLabel"]["value"]}
        for b in result["results"]["bindings"]
    ]

def fetch_bulk_recipients(qid):
    q = f"""
    SELECT ?person ?personLabel ?date WHERE {{
      ?person wdt:P31 wd:Q5.
      ?person p:P166 ?stmt.
      ?stmt ps:P166 wd:{qid}.
      ?stmt pq:P585 ?date.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}"""
    result = run_query(q)
    if not result: return {}
    year_map = {}
    for b in result["results"]["bindings"]:
        try:
            year = int(b["date"]["value"][:4])
            if not (1800 <= year <= datetime.now().year): continue
            person = b["person"]["value"].split("/")[-1]
            label = b.get("personLabel", {}).get("value", person)
            year_map.setdefault(str(year), []).append([label, person])
        except: continue
    return dict(sorted(year_map.items(), key=lambda x: int(x[0])))  # ⬅ 排序年份

def main():
    with open(AWARD_FILE, "r", encoding="utf-8") as f:
        awards = json.load(f)["award"]
    top_awards = sorted(awards, key=lambda x: -x["views_12m"])[:TOP_K]

    all_facts = {}
    all_mapping = {}

    for a in top_awards:
        label, qid = a["label"], a["qid"]
        print(f"\n🏁 {label} ({qid})")

        sub_awards = get_sub_awards(qid)
        # Always include parent award itself
        sub_awards.insert(0, {"qid": qid, "label": label})
        all_mapping[qid] = list({s["qid"] for s in sub_awards})  # 去重

        for s in sub_awards:
            sub_qid, sub_label = s["qid"], s["label"]
            print(f"  ↪️  {sub_label} ({sub_qid})")
            year_map = fetch_bulk_recipients(sub_qid)
            all_facts[sub_label] = {
                "qid": sub_qid,
                "parent_qid": qid,
                "years": year_map
            }
            time.sleep(SLEEP)

        # 每轮写入
        FACTS_FILE.write_text(json.dumps(all_facts, ensure_ascii=False, indent=2))
        MAP_FILE.write_text(json.dumps(all_mapping, ensure_ascii=False, indent=2))

    print(f"\n✅ All done. Saved to {FACTS_FILE.name} and {MAP_FILE.name}")

if __name__ == "__main__":
    main()
