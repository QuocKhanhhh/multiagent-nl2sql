import json
import requests

API_URL = "http://localhost:8002/ask"  # chỉnh theo docker-compose nếu cần

def normalize_rows(rows):
    norm = []
    for row in rows:
        new_row = []
        for x in row:
            try:
                val = float(x)
                # làm tròn để so sánh gần đúng
                val = round(val, 4)
                new_row.append(val)
            except:
                new_row.append(str(x).strip().lower())
        norm.append(tuple(new_row))
    return sorted(norm)

def evaluate_test(test, idx):
    question = test.get("question")
    gt_sql = test.get("ground_truth_sql")
    model_sql = None

    print(f"\n=== Test {idx+1} ===")
    print(f"Q: {question}")

    # 1. Lấy model SQL (từ question hoặc từ trường "sql")
    try:
        resp_model = requests.post(API_URL, json={"question": question})
        model_out = resp_model.json()
        model_sql = model_out.get("sql")
    except Exception as e:
        print("[ERROR] model call failed:", e)
        return False, False

    print("Model SQL:", model_sql)

    # 2. Kiểm tra valid SQL (nếu API trả về lỗi → invalid)
    valid = "error" not in model_out

    # 3. Semantic check (so sánh với ground truth)
    semantic_ok = False
    if gt_sql:
        try:
            # chạy ground truth
            resp_gt = requests.post(API_URL, json={"sql": gt_sql})
            gt_out = resp_gt.json()
            if "error" in gt_out:
                print("[WARN] GT SQL error:", gt_out["error"])
                return valid, False

            # normalize
            gt_rows = normalize_rows(gt_out["raw_result"]["rows"])
            model_rows = normalize_rows(model_out["raw_result"]["rows"])

            semantic_ok = (gt_rows == model_rows)
            if semantic_ok:
                print("[OK] Semantic match")
            else:
                print("[FAIL] Semantic mismatch")
                print("GT rows:", gt_rows)
                print("Model rows:", model_rows)
        except Exception as e:
            print("[ERROR] GT check failed:", e)

    return valid, semantic_ok


def main():
    with open("test_questions_1.json", "r", encoding="utf-8") as f:
        tests = json.load(f)

    total = len(tests)
    valid_count = 0
    semantic_count = 0

    for idx, test in enumerate(tests):
        valid, semantic_ok = evaluate_test(test, idx)
        if valid:
            valid_count += 1
        if semantic_ok:
            semantic_count += 1

    print("\n==== Evaluation Summary ====")
    print(f"Total questions: {total}")
    print(f"Valid SQL: {valid_count}/{total} ({valid_count/total*100:.1f}%)")
    print(f"Semantic correct: {semantic_count}/{total} ({semantic_count/total*100:.1f}%)")


if __name__ == "__main__":
    main()
