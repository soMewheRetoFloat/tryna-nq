import json, math, random
from tqdm import tqdm

FACTS_FILE = "structured_award_facts.json"
POP_FILE = "award_popularity.json"
SUBMAP_FILE = "award_sub_mapping.json"
OUTPUT_FILE = "qa_awards_sampled.jsonl"

QA_CNT = 1000
SPAN_RANGE = (3, 10)
MIN_ANSWERS = 2
MAX_TRIES_PER_AWARD = 20
PARENT_PROB = 0.2
MAX_FAILS = 3000

def compute_difficulty(span, _, avg_per_year, nonhuman_ratio):
    return round(
        0.4 * math.log1p(span) +
        0.6 * (2 - min(avg_per_year, 2)) +
        1.0 * nonhuman_ratio, 3
    )

def merge_facts_for_parent(qid, submap, facts):
    years = {}
    sub_qids = submap.get(qid, [])
    for sub_qid in sub_qids:
        sub_label = None
        for label, data in facts.items():
            if data["qid"] == sub_qid:
                sub_label = label
                break
        if not sub_label or sub_label not in facts:
            continue
        for year, people in facts[sub_label]["years"].items():
            years.setdefault(year, []).extend(people)
    return years

def main():
    facts = json.load(open(FACTS_FILE, encoding="utf-8"))
    submap = json.load(open(SUBMAP_FILE, encoding="utf-8"))
    pop_data = json.load(open(POP_FILE, encoding="utf-8"))["award"]
    qid_to_label = {a["qid"]: a["label"] for a in pop_data}

    print(f"🎯 Loaded {len(facts)} facts, {len(submap)} sub mappings, {len(qid_to_label)} popular labels")

    award_pool = []
    for label, data in facts.items():
        qid = data["qid"]
        parent_qid = data["parent_qid"]
        is_parent = (qid == parent_qid)
        if qid in qid_to_label:
            award_pool.append((label, qid, is_parent))

    parents = [x for x in award_pool if x[2]]
    children = [x for x in award_pool if not x[2]]
    print(f"✅ parent: {len(parents)} | child: {len(children)}")

    candidates = []
    fail_counter = 0
    pbar = tqdm(total=QA_CNT, desc="Generating QA")

    while len(candidates) < QA_CNT:
        if fail_counter >= MAX_FAILS:
            print("❌ Too many failures. Exiting.")
            break

        pool = parents if random.random() < PARENT_PROB else children
        if not pool:
            pool = children or parents
        label, qid, is_parent = random.choice(pool)

        if is_parent:
            years = merge_facts_for_parent(qid, submap, facts)
        else:
            years = facts[label]["years"]

        year_keys = sorted([y for y in years if y != "unknown"], key=int)
        if not year_keys:
            fail_counter += 1
            continue

        generated = False
        for _ in range(MAX_TRIES_PER_AWARD):
            span = random.randint(*SPAN_RANGE)
            if len(year_keys) <= span:
                continue
            valid_starts = year_keys[:-(span - 1)]
            if not valid_starts:
                continue
            start = random.choice(valid_starts)
            end = str(int(start) + span)
            window = [y for y in year_keys if start <= y <= end]
            all_entities = sum([years[y] for y in window], [])
            if len(all_entities) < MIN_ANSWERS:
                continue

            nonhuman = sum(1 for e in all_entities if e[2] != "Q5")
            ratio = nonhuman / len(all_entities)
            avg_per_year = len(all_entities) / len(window)

            candidates.append({
                "question": f"Who won the {label} between {start} and {end}?",
                "answers": [[e[0], e[1]] for e in all_entities],
                "answer_count": len(all_entities),
                "meta": {
                    "award": label,
                    "award_qid": qid,
                    "is_parent": is_parent,
                    "year_range": [int(start), int(end)],
                    "nonhuman_ratio": round(ratio, 3),
                    "avg_per_year": round(avg_per_year, 2),
                    "span": int(end) - int(start) + 1
                },
                "difficulty": compute_difficulty(
                    int(end) - int(start) + 1, 0, avg_per_year, ratio
                )
            })
            print(f"✅ {label} ({qid}) → {start}–{end}, {len(all_entities)} entities")
            pbar.update(1)
            fail_counter = 0
            generated = True
            break

        if not generated and "unknown" in years and random.random() < 0.05:
            all_entities = years["unknown"]
            if len(all_entities) >= MIN_ANSWERS:
                nonhuman = sum(1 for e in all_entities if e[2] != "Q5")
                ratio = nonhuman / len(all_entities)
                avg = len(all_entities)
                candidates.append({
                    "question": f"Who has won the {label}?",
                    "answers": [[e[0], e[1]] for e in all_entities],
                    "answer_count": len(all_entities),
                    "meta": {
                        "award": label,
                        "award_qid": qid,
                        "is_parent": is_parent,
                        "year_range": "unknown",
                        "nonhuman_ratio": round(ratio, 3),
                        "avg_per_year": round(avg, 2),
                        "span": 0
                    },
                    "difficulty": compute_difficulty(0, 0, avg, ratio)
                })
                print(f"🟡 {label} ({qid}) → unknown year, {len(all_entities)} entities")
                pbar.update(1)
                fail_counter = 0
            else:
                fail_counter += 1
        elif not generated:
            fail_counter += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fout:
        for item in candidates:
            fout.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"\n✅ Finished. {len(candidates)} QA written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
