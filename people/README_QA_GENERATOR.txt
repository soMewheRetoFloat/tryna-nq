
QA 生成器说明
=============

功能：
-----
从以下三个属性组中，随机组合生成问题并查询实体作为答案，输出 QA 对 JSONL 文件（含答案数量）：

1. pam1: 时间限制（出生/死亡年份）
2. pam2: 国籍 / 语言 / 政党 / 宗教（加权采样）
3. pam3: 职业或工作领域（均匀采样）

采样方式：
---------
- pam1: 随机选取一个年份（例如 1890），构造出生或死亡条件（P569/P570）
- pam2: 从访问量 JSON（country/language/party/religion）中加权抽样一个值
- pam3: 从 occupation_qid.json + field_qid.json 中均匀选一个

自然语言生成：
--------------
- pam1: "born in {year}" or "died in {year}"
- pam2: 根据属性自定义自然语言（如 "from the United States", "who speak German"）
- pam3: 如职业 "who are actors"，领域 "who work in chemistry"

输出：
-----
每条 JSONL 结构如下：
{
  "question": "...",
  "filters": {
    "P569": "1890",
    "P27": "Q30",
    "P106": "Q33999"
  },
  "answers": [...],
  "answer_count": 87
}

运行：
-----
python generate_qa.py

需要文件：
------------
- occupation_qid.json
- field_qid.json
- country_popularity.json
- language_popularity.json
- party_popularity.json
- religion_popularity.json
