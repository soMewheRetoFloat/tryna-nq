import json
import random
import uuid

QA_CNT = 100  # 每类题目数量
N = 5         # 每题问几个（如 5 个省/5 个城市）

def load_data(json_path):
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)

def get_country_list(data):
    return list(data.keys())

def get_provinces(country_data):
    return country_data.get("subdivisions", [])

def get_all_cities_in_country(country_data):
    cities = []
    for prov in country_data.get("subdivisions", []):
        for city in prov.get("children", []):
            cities.append(city)
    return cities

def get_cities_in_province(province):
    return province.get("children", [])

def get_capital(country_data):
    cap = country_data.get("capital")
    if cap:
        return cap.get("label")
    return None

def get_country_views(country_data):
    return country_data.get("views_12m", None)

def get_province_views(province):
    return province.get("views_12m", None)

def gen_province_question(data, n=N):
    tries = 0
    while tries < 10:
        country = random.choice(get_country_list(data))
        provinces = get_provinces(data[country])
        if not provinces or len(provinces) < 1:
            tries += 1
            continue
        pick_n = min(n, len(provinces))
        q = f"Name {pick_n} provinces/states of {country}."
        ans = [prov["label"] for prov in provinces]
        core = country
        views = get_country_views(data[country])
        return {
            "id": str(uuid.uuid4()),
            "type": "country_provinces",
            "question": q,
            "answer": ans,
            "core": country,
            "core_views": views
        }
    return None

def gen_country_city_question(data, n=N):
    tries = 0
    while tries < 10:
        country = random.choice(get_country_list(data))
        cities = get_all_cities_in_country(data[country])
        if not cities or len(cities) < 1:
            tries += 1
            continue
        pick_n = min(n, len(cities))
        q = f"Name {pick_n} cities in {country}."
        ans = [city["label"] for city in cities]
        core = country
        views = get_country_views(data[country])
        return {
            "id": str(uuid.uuid4()),
            "type": "country_cities",
            "question": q,
            "answer": ans,
            "core": country,
            "core_views": views
        }
    return None

def gen_province_city_question(data, n=N):
    tries = 0
    while tries < 10:
        country = random.choice(get_country_list(data))
        provinces = get_provinces(data[country])
        if not provinces:
            tries += 1
            continue
        # 只选有城市的省
        provinces_with_cities = [p for p in provinces if p.get("children")]
        if not provinces_with_cities:
            tries += 1
            continue
        province = random.choice(provinces_with_cities)
        cities = get_cities_in_province(province)
        if not cities:
            tries += 1
            continue
        pick_n = min(n, len(cities))
        q = f"Name {pick_n} cities in {province['label']} ({country})."
        ans = [city["label"] for city in cities]
        views = get_province_views(province)
        return {
            "id": str(uuid.uuid4()),
            "type": "province_cities",
            "question": q,
            "answer": ans,
            "core": province['label'],
            "core_views": views
        }
    return None

def gen_capital_of_n_countries(data, n=3):
    tries = 0
    while tries < 10:
        countries = random.sample(get_country_list(data), n)
        capitals = [get_capital(data[c]) for c in countries if get_capital(data[c])]
        views = [get_country_views(data[c]) for c in countries]
        if len(capitals) > 0:
            q = "Name one capital city from the following countries: " + ", ".join(countries) + "."
            ans = capitals
            return {
                "id": str(uuid.uuid4()),
                "type": "capital_of_countries",
                "question": q,
                "answer": ans,
                "core": countries,
                "core_views": views
            }
        tries += 1
    return None

def batch_generate(data, outpath, n=N):
    results = []
    print("Generating Q1...")
    for _ in range(QA_CNT):
        res = gen_province_question(data, n)
        if res:
            results.append(res)
    print("Generating Q2...")
    for _ in range(QA_CNT):
        res = gen_country_city_question(data, n)
        if res:
            results.append(res)
    print("Generating Q3...")
    for _ in range(QA_CNT):
        res = gen_province_city_question(data, n)
        if res:
            results.append(res)
    print("Generating Q4...")
    for _ in range(QA_CNT):
        res = gen_capital_of_n_countries(data, n=3)
        if res:
            results.append(res)
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(results)} questions to {outpath}")

if __name__ == "__main__":
    data = load_data("subdivisions_tree_postprocessed.json")
    batch_generate(data, "geo_questions.json", n=N)
