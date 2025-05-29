import json

with open('structured_award_facts.json', encoding='utf-8') as f:
    award_facts = json.load(f)

fixed_count_awards = {}

for award, info in award_facts.items():
    # 只用有数字的年份，不含 "unknown"
    year_counts = [len(v) for y, v in info['years'].items() if y.isdigit() and len(v) > 0]
    # 至少两年，且所有年份人数都一样且非0
    if len(year_counts) >= 2 and len(set(year_counts)) == 1:
        fixed_count_awards[award] = {
            'qid': info['qid'],
            'parent_qid': info.get('parent_qid'),
            'count_per_year': year_counts[0],
            'years': {y: v for y, v in info['years'].items() if y.isdigit() and len(v) > 0}
        }

print(f"筛选出{len(fixed_count_awards)}个每年固定人数奖项")
with open('fixed_count_awards.json', 'w', encoding='utf-8') as f:
    json.dump(fixed_count_awards, f, ensure_ascii=False, indent=2)
