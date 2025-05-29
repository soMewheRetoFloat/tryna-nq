#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_qa.py
--------------
从 pam1 + pam2 + pam3 中采样生成 QA 问题，并通过 SPARQL 查询实体答案。
输出为 JSONL，每行包含问题、筛选条件、答案和答案数。
"""

QA_CNT = 10

import json, random
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

# 加载配置文件
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# SPARQL 设置
WDQS = "https://query.wikidata.org/sparql"
sparql = SPARQLWrapper(WDQS, agent="PopPop/qa-gen 1.0")
sparql.setReturnFormat(JSON)

# pam1: 年份范围
YEAR_RANGE = list(range(1940, 2001))

# pam2 属性及其来源文件和内部键
PAM2_SOURCES = {
    "P27": ("country_popularity.json", "from {label}", "country"),
    "P1412": ("language_popularity.json", "who speak {label}", "language"),
    # "P102": ("party_popularity.json", "affiliated with the {label}", "party"),
    "P140": ("religion_popularity.json", "who follow {label}", "religion")
}

# pam3 文件
OCCUPATION_FILE = "occupation_qid.json"
FIELD_FILE = "field_qid.json"

# 构造 SPARQL 查询语句
def build_query(filters):
    parts = ["?person wdt:P31 wd:Q5 ."]
    for pid, val in filters.items():
        if pid in ["P569", "P570"]:
            parts.append(f"?person wdt:{pid} ?date . FILTER(YEAR(?date) = {val})")
        else:
            parts.append(f"?person wdt:{pid} wd:{val} .")
    return f"""
    SELECT ?person ?personLabel WHERE {{
    {' '.join(parts)}
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}"""

# 查询答案
def query_answers(filters):
    query = build_query(filters)
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
        bindings = results["results"]["bindings"]
        return [
            (
                b["personLabel"]["value"] if "personLabel" in b else b["person"]["value"].split("/")[-1],
                b["person"]["value"].split("/")[-1]
            )
            for b in bindings
        ]
    except Exception as e:
        return []

# 加权采样（从访问量列表中抽一个）
def weighted_choice(entries):
    total = sum(e["views_12m"] for e in entries)
    r = random.uniform(0, total)
    upto = 0
    for e in entries:
        upto += e["views_12m"]
        if upto >= r:
            return e
    return entries[-1]

# 主程序
def main():
    # random.seed(42)

    # 加载 pam2 权重表（只取前10）
    pam2_data = {}
    for pid, (fname, _, inner_key) in PAM2_SOURCES.items():
        pam2_data[pid] = load_json(fname)[inner_key][:10]  # 限定前10热门

    # 加载 pam3 候选
    occ = load_json(OCCUPATION_FILE)
    # fld = load_json(FIELD_FILE)
    pam3_pool = list(occ.items()) 
    # + list(fld.items())

    # 生成 QA
    with open("generated_qa.jsonl", "w", encoding="utf-8") as fout:
        for _ in tqdm(range(QA_CNT), desc="Generating QA"):
            filters = {}
            desc_parts = []

            # pam1
            year = random.choice(YEAR_RANGE)
            pid1 = random.choice(["P569", "P570"])
            filters[pid1] = str(year)
            desc_parts.append(("born in" if pid1 == "P569" else "died in") + f" {year}")

            # pam2
            pid2 = random.choice(list(PAM2_SOURCES.keys()))
            _, template, _ = PAM2_SOURCES[pid2]
            entry = weighted_choice(pam2_data[pid2])
            filters[pid2] = entry["qid"]
            desc_parts.append(template.format(label=entry["label"]))

            # pam3
            label3, qid3 = random.choice(pam3_pool)
            is_occ = label3 in occ
            pid3 = "P106" if is_occ else "P101"
            filters[pid3] = qid3
            desc_parts.append(f"who are {label3}" if is_occ else f"who work in {label3}")

            # 问题 & 查询
            question = "Which people " + ", ".join(desc_parts) + "?"
            answers = query_answers(filters)

            fout.write(json.dumps({
                "question": question,
                "filters": filters,
                "answers": answers,
                "answer_count": len(answers)
            }, ensure_ascii=False) + "\n")

    print("✓ QA 生成完成，写入 generated_qa.jsonl")

if __name__ == "__main__":
    main()
