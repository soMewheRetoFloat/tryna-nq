import json
import math
import random

# === 各类数列生成器 ===
def generate_primes(upto):
    sieve = [True] * (upto + 1)
    sieve[0:2] = [False, False]
    for num in range(2, int(upto**0.5) + 1):
        if sieve[num]:
            sieve[num*num:upto+1:num] = [False]*len(range(num*num, upto+1, num))
    return [i for i, val in enumerate(sieve) if val]

def generate_squares(upto):
    return [i*i for i in range(1, int(upto**0.5) + 1)]

def generate_cubes(upto):
    return [i**3 for i in range(1, int(upto**(1/3)) + 2) if i**3 <= upto]

def generate_fibonacci(upto):
    fibs = [1, 1]
    while fibs[-1] + fibs[-2] <= upto:
        fibs.append(fibs[-1] + fibs[-2])
    return fibs

def generate_triangular(upto):
    tris = []
    i = 1
    while True:
        t = i * (i + 1) // 2
        if t > upto:
            break
        tris.append(t)
        i += 1
    return tris

# === 类型映射 ===
number_generators = {
    "prime number": generate_primes,
    "square number": generate_squares,
    "cube number": generate_cubes,
    "Fibonacci number": generate_fibonacci,
    "triangular number": generate_triangular,
}

# === 双边题目生成函数 ===
def generate_between_question(number_type, solution_count, max_bound=10000):
    if number_type not in number_generators:
        raise ValueError(f"Unsupported number type: {number_type}")
    
    all_numbers = number_generators[number_type](max_bound)
    
    for _ in range(1000):
        a = random.randint(1, max_bound - 100)
        b = random.randint(a + 10, a + 500)
        filtered = [x for x in all_numbers if a <= x < b]
        if len(filtered) == solution_count:
            question = f"Name a {number_type} between {a} and {b}."
            return {
                "question": question,
                "answers": filtered,
                "type": number_type,
                "range": [a, b]
            }
    
    raise ValueError(f"Unable to generate question with {solution_count} solutions for {number_type}")

# === 主函数：批量生成 QA 对 ===
QA_CNT = 20  # 修改这个值以控制生成题目数量

def generate_dataset():
    results = []
    for _ in range(QA_CNT):
        num_type = random.choice(list(number_generators.keys()))
        sol_count = random.choice([1, 2, 3, 4, 5])
        try:
            qa = generate_between_question(num_type, sol_count)
            results.append(qa)
        except ValueError:
            continue
    return results

# === 主执行 ===
if __name__ == "__main__":
    dataset = generate_dataset()
    with open("math_between_questions.json", "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2)
    print("Saved to math_between_questions.json")
