from smart_io import SmartIO
import os
import time
import json

sio = SmartIO()

dummy_data = [{"id": i, "value": f"test_{i}"} for i in range(1000)]

print("--- 测试 JSON 写入与读取 ---")
start = time.time()
sio.write("test.json", dummy_data, indent=2)
print(f"使用SIO写入 JSON 文件耗时 {time.time() - start} 秒")

os.remove("test.json")

start = time.time()
with open("test.json", "w") as f:
    json.dump(dummy_data, f, indent=2)
print(f"使用Python写入 JSON 文件耗时 {time.time() - start} 秒")


content = sio.read("test.json")
print(f"读取到 {len(content)} 条 JSON 记录")