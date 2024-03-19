import requests
import time

orchestrator_api_url = "http://localhost:8080"

load_test_config = {
    "target_url": "http://localhost:5000/ping",
    "target_throughput": 5,
    "total_requests": 50
}

response = requests.post(f"{orchestrator_api_url}/start-test", json=load_test_config)
print(response.json())

time.sleep(5)
response = requests.get(f"{orchestrator_api_url}/get-statistics")
print(response.json())

response = requests.post(f"{orchestrator_api_url}/stop-test")
print(response.json())
