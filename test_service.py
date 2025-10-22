import requests
import json
import time
import subprocess


def wait_for_server(url, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, params={"user_id": 0})
            if r.status_code in (200, 422):
                print("Сервер запущен")
                return True
        except requests.exceptions.ConnectionError:
            time.sleep(1)
    print("Сервер не запустился за отведённое время")
    return False

process = subprocess.Popen(
    ["uvicorn", "recommendations_service:app", "--port", "8000"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

base_url = "http://127.0.0.1:8000/recommendations"

if not wait_for_server(base_url):
    process.terminate()
    raise SystemExit("Сервис не запущен — проверь recommendations_service.py")

tests = {
    "no_personal_recs": 999999,
    "only_offline": 0,
    "offline_and_online": 0,
}

results = {}
for name, user_id in tests.items():
    try:
        r = requests.get(base_url, params={"user_id": user_id})
        results[name] = {
            "status_code": r.status_code,
            "response": r.json()
        }
        print(f"{name}: OK")
    except Exception as e:
        results[name] = {"error": str(e)}
        print(f"Ошибка в тесте {name}: {e}")

process.terminate()

with open("test_service.log", "w", encoding="utf-8") as f:
    f.write(json.dumps(results, ensure_ascii=False, indent=4))

print("Тестирование завершено, результаты в test_service.log")
