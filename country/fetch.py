# fetch_subdivisions_area_capital_concurrent.py

import json, sys, time, re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm

WD_ENDPOINT = "https://query.wikidata.org/sparql"
ADM1_CLASS = "wd:Q10864048"   # first-level administrative division
CITY_CLASS = "wd:Q515"        # city
AREA_PROP = "P2046"           # area
CAPITAL_PROP = "P36"          # capital

def parse_quantity(val):
    if val.startswith("http://www.wikidata.org/.well-known/genid/"):
        return "unknown"
    try:
        return float(val)
    except Exception:
        return None

def run_query(query: str) -> List[Dict]:
    sparql = SPARQLWrapper(WD_ENDPOINT)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "SubdivFetcher/1.0 (+https://chat.openai.com/)")
    for _ in range(3):
        try:
            sparql.setQuery(query)
            time.sleep(1.05)
            return sparql.query().convert()["results"]["bindings"]
        except Exception as exc:
            print("SPARQL error, retrying:", exc, file=sys.stderr)
            time.sleep(5)
    return []

def get_country_capital(country_qid):
    query = f"""
    SELECT ?capital ?capitalLabel WHERE {{
      wd:{country_qid} wdt:{CAPITAL_PROP} ?capital.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    rows = run_query(query)
    if rows:
        return {
            "qid": rows[0]["capital"]["value"].split("/")[-1],
            "label": rows[0]["capitalLabel"]["value"]
        }
    return None

def fetch_country_tree(country_qid: str, country_label: str):
    # Main query: fetch province, province area/capital, city, city area
    query = f"""
    SELECT DISTINCT
           ?province ?provinceLabel ?provArea ?provCapital ?provCapitalLabel
           ?city ?cityLabel ?cityArea
    WHERE {{
      ?province wdt:P31/wdt:P279* {ADM1_CLASS} ;
                wdt:P17 wd:{country_qid} .
      OPTIONAL {{ ?province wdt:{AREA_PROP} ?provArea . }}
      OPTIONAL {{ ?province wdt:{CAPITAL_PROP} ?provCapital .
                  ?provCapital rdfs:label ?provCapitalLabel .
                  FILTER(LANG(?provCapitalLabel) = "en")
                }}

      ?city wdt:P31/wdt:P279* {CITY_CLASS} ;
            wdt:P131+ ?province .
      OPTIONAL {{ ?city wdt:{AREA_PROP} ?cityArea . }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    rows = run_query(query)
    # Build province dict: QID -> node
    provinces: Dict[str, Dict] = {}
    province_city_seen = defaultdict(set)

    for r in rows:
        prov_qid = r["province"]["value"].split("/")[-1]
        prov_label = r["provinceLabel"]["value"]
        prov_area = parse_quantity(r["provArea"]["value"]) if "provArea" in r and r["provArea"]["value"] else None

        prov_cap_qid = r["provCapital"]["value"].split("/")[-1] if "provCapital" in r else None
        prov_cap_label = r["provCapitalLabel"]["value"] if "provCapitalLabel" in r else None
        prov_capital = {"qid": prov_cap_qid, "label": prov_cap_label} if prov_cap_qid and prov_cap_label else None

        # province node: only set if not exists
        prov_node = provinces.get(prov_qid)
        if prov_node is None:
            prov_node = {
                "qid": prov_qid,
                "label": prov_label,
                "children": []
            }
            if prov_area is not None:
                prov_node["area_km2"] = prov_area
            prov_node["capital"] = prov_capital if prov_capital else None
            provinces[prov_qid] = prov_node
        else:
            # update area/capital if not yet set
            if prov_area is not None and "area_km2" not in prov_node:
                prov_node["area_km2"] = prov_area
            if prov_capital and not prov_node.get("capital"):
                prov_node["capital"] = prov_capital

        # City
        if "city" not in r or not r["city"]["value"]:
            continue
        city_qid = r["city"]["value"].split("/")[-1]
        if city_qid in province_city_seen[prov_qid]:
            continue
        province_city_seen[prov_qid].add(city_qid)

        city_label = r["cityLabel"]["value"] if "cityLabel" in r else None
        city_area = parse_quantity(r["cityArea"]["value"]) if "cityArea" in r and r["cityArea"]["value"] else None
        city_node = {
            "qid": city_qid,
            "label": city_label
        }
        if city_area is not None:
            city_node["area_km2"] = city_area
        prov_node["children"].append(city_node)

    # 保证每个省都有children字段
    for prov in provinces.values():
        if "children" not in prov:
            prov["children"] = []
        # capital一定有，没有就写None
        if "capital" not in prov:
            prov["capital"] = None

    return country_label, {"qid": country_qid, "subdivisions": list(provinces.values())}

def main(src: str, dst: str, max_workers: int = 5):
    data = json.loads(Path(src).read_text(encoding="utf-8"))
    countries = data.get("country", [])
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 先查所有国家的capital
        capitals = {}
        for c in tqdm(countries, desc="Fetch country capitals"):
            capitals[c["label"]] = get_country_capital(c["qid"])
        # 再并发查细分
        future_to_country = {
            executor.submit(fetch_country_tree, c["qid"], c["label"]): c["label"]
            for c in countries
        }
        for i, future in enumerate(tqdm(as_completed(future_to_country), total=len(future_to_country), desc="Countries")):
            try:
                label, country_data = future.result()
                country_data["capital"] = capitals[label]
                results[label] = country_data
                print(f"[{i+1}/{len(future_to_country)}] {label} done.")
            except Exception as e:
                label = future_to_country[future]
                print(f"[{i+1}/{len(future_to_country)}] {label} failed: {e}", file=sys.stderr)
                results[label] = {"qid": None, "subdivisions": [], "capital": None}

    Path(dst).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✓ Saved", dst)

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python fetch_subdivisions_area_capital_concurrent.py input.json output.json [max_workers]", file=sys.stderr)
        sys.exit(1)
    src, dst = sys.argv[1], sys.argv[2]
    max_workers = int(sys.argv[3]) if len(sys.argv) == 4 else 5
    main(src, dst, max_workers)
