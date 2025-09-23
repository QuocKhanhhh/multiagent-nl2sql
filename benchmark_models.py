import time
import json
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from difflib import SequenceMatcher

# ===== Config =====
TEST_FILE = "test_questions.json"   # file chứa bộ câu hỏi test
MODELS = [
    "google/flan-t5-small",
    "google/flan-t5-base",
    "tiiuae/falcon-rw-1b"
    # thêm model khác nếu muốn
]
DEVICE = "cpu"  # hoặc "cuda" nếu có GPU
MAX_NEW_TOKENS = 128


def load_tests():
    with open(TEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def sql_similarity(sql1, sql2):
    """Đo mức độ giống nhau giữa 2 câu SQL (chuẩn hóa lowercase, bỏ khoảng trắng thừa)."""
    s1 = " ".join(sql1.lower().split())
    s2 = " ".join(sql2.lower().split())
    return SequenceMatcher(None, s1, s2).ratio()


def benchmark_model(model_name, tests):
    print(f"\n=== Benchmark {model_name} ===")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSeq2SeqLM.from_pretrained(model_name, device_map="cpu")

    nlp = pipeline("text2text-generation", model=model, tokenizer=tokenizer, device=-1)

    total_time = 0
    correct = 0
    results = []

    for t in tests:
        q = t["question"]
        expected = t["expected_sql"]

        prompt = f"Viết câu SQL để trả lời câu hỏi: {q}"

        start = time.time()
        out = nlp(prompt, max_new_tokens=MAX_NEW_TOKENS, do_sample=False)[0]["generated_text"]
        elapsed = time.time() - start
        total_time += elapsed

        sim = sql_similarity(out, expected)
        ok = sim > 0.7  # coi là đúng nếu giống nhau trên 70%
        if ok:
            correct += 1

        results.append({
            "question": q,
            "expected": expected,
            "generated": out,
            "similarity": sim,
            "latency_sec": elapsed
        })

    accuracy = correct / len(tests)
    avg_latency = total_time / len(tests)

    print(f"Accuracy: {accuracy:.2%}")
    print(f"Avg Latency: {avg_latency:.2f} sec")
    return {
        "model": model_name,
        "accuracy": accuracy,
        "avg_latency": avg_latency,
        "results": results
    }


def main():
    tests = load_tests()
    summary = []
    for m in MODELS:
        res = benchmark_model(m, tests)
        summary.append(res)

    print("\n=== Tổng kết ===")
    for r in summary:
        print(f"{r['model']}: accuracy={r['accuracy']:.2%}, latency={r['avg_latency']:.2f}s")


if __name__ == "__main__":
    main()
