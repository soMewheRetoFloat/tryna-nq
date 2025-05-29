
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_popularity_entities.py
-----------------------------
▸ Country    — Q6256
▸ Language   — Q34770
▸ Political Party — Q7278
每类实体抓取其 enwiki 页面，查询过去 12 个月页面访问量并存储。
"""

import asyncio, aiohttp, datetime, json, logging, re, sys
from typing import Dict, List
from pythonjsonlogger import jsonlogger
from SPARQLWrapper import SPARQLWrapper, JSON, POST

LOG_FILE = "popularity_entities.log"
UA = "PopPop/c4freq 1.2 (email@example.com)"
PV_CONC = 50



ENTITY_CONFIG = {
# "country":  {"qid": "Q6256",  "outfile": "country_popularity.json"},
# "language": {"qid": "Q34770", "outfile": "language_popularity.json"},
# "party":    {"qid": "Q7278",  "outfile": "party_popularity.json"},
# "religion": {"qid": "Q9174", "outfile": "religion_popularity.json"},
"office": {"qid": "Q4164871", "outfile": "office_popularity.json"},
# "award":   {"qid": "Q618779", "outfile": "award_popularity.json"},
}

# ENTITY_CONFIG = {
    
# }

# ---------- Logging ----------
log = logging.getLogger("pop"); log.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE); fh.setFormatter(jsonlogger.JsonFormatter())
sh = logging.StreamHandler(sys.stdout); sh.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(fh); log.addHandler(sh)

# ---------- WDQS ----------
WDQS = "https://query.wikidata.org/sparql"
def run(query: str) -> List[Dict]:
    s = SPARQLWrapper(WDQS, agent=UA)
    s.setMethod(POST)
    s.setQuery(query)
    s.setReturnFormat(JSON)
    return s.query().convert()["results"]["bindings"]

def fetch_basic(qid: str) -> List[Dict]:
    query = f"""
    SELECT ?e ?eLabel ?art WHERE {{
      ?e wdt:P31 wd:{qid} .
      ?art schema:about ?e ; schema:isPartOf <https://en.wikipedia.org/> ;
           schema:inLanguage "en".
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" }}
    }}
    """
    rows = run(query)
    uniq = {}
    for r in rows:
        qid = r["e"]["value"].split("/")[-1]
        if qid in uniq: continue
        uniq[qid] = {
            "qid": qid,
            "label": r["eLabel"]["value"],
            "title": r["art"]["value"].split("/")[-1]
        }
    return list(uniq.values())

async def _pv_12m(sess, title, end_dt):
    start_dt = end_dt.replace(year=end_dt.year - 1) + datetime.timedelta(days=1)
    s, e = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    url = ( "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia.org/all-access/all-agents/{title}/monthly/{s}/{e}" )
    try:
        async with sess.get(url, timeout=20) as r:
            if r.status != 200: return 0
            items = (await r.json()).get("items", [])
            return sum(it["views"] for it in items)
    except Exception:
        return 0

async def fill_views_12m(items):
    end_dt = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
    conn = aiohttp.TCPConnector(limit=PV_CONC)
    async with aiohttp.ClientSession(headers={"User-Agent": UA}, connector=conn) as sess:
        tasks = [_pv_12m(sess, c["title"], end_dt) for c in items]
        for c, v in zip(items, await asyncio.gather(*tasks)):
            c["views_12m"] = v

async def process_entity(name, cfg):
    log.info({"phase": f"{name}_list"})
    items = fetch_basic(cfg["qid"])
    await fill_views_12m(items)
    items = [x for x in items if x["views_12m"] > 0]
    items.sort(key=lambda x: -x["views_12m"])
    as_of = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
    with open(cfg["outfile"], "w", encoding="utf-8") as f:
        json.dump({"as_of": as_of, name: items}, f, ensure_ascii=False, indent=2)
    log.info({"phase": "save", "file": cfg["outfile"], "as_of": as_of})
    print(f"✓ {name}: 完成，写入 {cfg['outfile']}")

async def main():
    for name, cfg in ENTITY_CONFIG.items():
        await process_entity(name, cfg)

if __name__ == "__main__":
    asyncio.run(main())
