
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_field_qid.py
------------------
▸ 获取 field of work 候选实体列表（Q11862829 为父类）
▸ 保存为 dict 格式：{label: qid}
"""

from SPARQLWrapper import SPARQLWrapper, JSON, POST
import json

UA = "PopPop/c4freq 1.2 (email@example.com)"
OUT_FILE = "field_qid.json"
WDQS = "https://query.wikidata.org/sparql"

def run(query: str):
    s = SPARQLWrapper(WDQS, agent=UA)
    s.setMethod(POST)
    s.setQuery(query)
    s.setReturnFormat(JSON)
    return s.query().convert()["results"]["bindings"]

def fetch_field_entities():
    query = """
    SELECT DISTINCT ?f ?fLabel WHERE {
      ?f wdt:P31 wd:Q11862829 .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
    }
    """
    rows = run(query)
    result = {}
    for r in rows:
        label = r["fLabel"]["value"]
        qid = r["f"]["value"].split("/")[-1]
        result[label] = qid
    return result

def main():
    result = fetch_field_entities()
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved {len(result)} fields to", OUT_FILE)

if __name__ == "__main__":
    main()
