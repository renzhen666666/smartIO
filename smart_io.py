import os
import sys
import mmap
import gc
import shutil
from pathlib import Path
import json



import csv
import orjson
import pandas as pd


class SmartIOConfig:
    """配置类：定义阈值"""
    # 超过这个大小，视为大文件（默认 100MB）
    LARGE_FILE_THRESHOLD = 100 * 1024 * 1024 
    # 内存占用阈值：文件大小超过可用内存的 30% 时，强制使用流式/Mmap，避免OOM
    MEMORY_USAGE_RATIO = 0.3

class SmartIO:
    def __init__(self):
        self.mem_total = self._get_system_memory()

    def _get_system_memory(self):
        """获取系统总内存（字节）"""
        try:
            import psutil
            return psutil.virtual_memory().total
        except ImportError:
            # 如果没有 psutil，给一个保守的估计值 (例如 8GB)
            return 8 * 1024 * 1024 * 1024

    def _is_large_file(self, file_size):
        """判断是否为大文件"""
        return file_size > SmartIOConfig.LARGE_FILE_THRESHOLD

    def _is_memory_risky(self, file_size):
        """判断文件大小是否对内存有风险"""
        return file_size > (self.mem_total * SmartIOConfig.MEMORY_USAGE_RATIO)

    def read(self, filepath, mode='r', **kwargs):
        """
        智能读取入口
        :param filepath: 文件路径
        :param mode: 读取模式 ('r' 文本, 'rb' 二进制, 'df' 返回DataFrame)
        :return: 读取的内容
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File {filepath} not found")

        file_size = path.stat().st_size
        ext = path.suffix.lower()

        # --- 策略 1: 结构化数据 (JSON) ---
        if ext == '.json':
            return self._read_json(path, mode, file_size)
        
        # --- 策略 2: 表格数据 (CSV) ---
        if ext == '.csv':
            return self._read_csv(path, file_size, **kwargs)

        # --- 策略 3: 通用二进制/文本 ---
        is_binary = 'b' in mode
        
        # 如果是二进制且是大文件，优先使用 mmap
        if is_binary and (self._is_large_file(file_size) or self._is_memory_risky(file_size)):
            return self._read_mmap(path, kwargs.get('encoding'))

        # 如果是文本大文件，使用流式读取
        if not is_binary and (self._is_large_file(file_size) or self._is_memory_risky(file_size)):
            return self._read_stream(path, kwargs.get('encoding'))

        # 默认：小文件直接全量读取
        return self._read_simple(path, mode, kwargs.get('encoding'))

    def write(self, filepath, data, mode='w', **kwargs):
        """
        智能写入入口
        """
        path = Path(filepath)
        ext = path.suffix.lower()

        # --- JSON 写入 ---
        if ext == '.json':
            return self._write_json(path, data, mode, kwargs.get('indent'))

        # --- CSV 写入 ---
        if ext == '.csv':
            return self._write_csv(path, data, mode)

        # --- 通用写入 ---
        return self._write_simple(path, data, mode, kwargs.get('encoding'))

    # ================= 内部实现细节 =================

    def _read_json(self, path, mode, file_size):
        """JSON 专用读取逻辑"""

        
        # orjson 直接读取 bytes，比 decode 成 str 再读快得多
        with open(path, 'rb') as f:
            return orjson.loads(f.read())

    def _write_json(self, path, data, mode, indent):
        """JSON 专用写入逻辑"""

        # orjson 默认 dump bytes
        option = orjson.OPT_INDENT_2 if indent else 0
        with open(path, 'wb') as f:
            f.write(orjson.dumps(data, option=option))

    def _read_csv(self, path, file_size, **kwargs):
        """CSV 专用读取逻辑"""

        # Pandas 自动处理了内存优化和解析速度
        return pd.read_csv(path, **kwargs)

    def _write_csv(self, path, data, mode):
        """CSV 写入逻辑"""
        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
            return
        
        # 简单的列表写入
        
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def _read_mmap(self, path, encoding):
        """使用 mmap 读取大文件（仅限二进制模式演示）"""
        with open(path, 'r+b') as f:
            # mmap.fileno() 将文件映射到内存
            # access=mmap.ACCESS_READ 表示只读，不复制到内存，直接操作磁盘文件映射
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                return mm.read() # 返回 bytes

    def _read_stream(self, path, encoding):
        """流式读取大文本文件（生成器模式）"""
        encoding = encoding or 'utf-8'
        with open(path, 'r', encoding=encoding) as f:
            # 这是一个生成器，不会一次性吃爆内存
            for line in f:
                yield line

    def _read_simple(self, path, mode, encoding):
        """简单小文件读取"""
        encoding = encoding or 'utf-8'
        with open(path, mode, encoding=encoding) as f:
            return f.read()

    def _write_simple(self, path, data, mode, encoding):
        """简单写入"""
        encoding = encoding or 'utf-8'
        # 简单的缓冲区优化
        with open(path, mode, encoding=encoding, buffering=1024*1024) as f:
            f.write(data)

# ================= 使用示例 =================

if __name__ == "__main__":
    sio = SmartIO()

    # 模拟文件创建
    dummy_data = [{"id": i, "value": f"test_{i}"} for i in range(1000)]
    
    print("--- 测试 JSON 写入与读取 ---")
    sio.write("test.json", dummy_data, indent=2)
    content = sio.read("test.json")
    print(f"读取到 {len(content)} 条 JSON 记录")

    print("\n--- 测试 CSV 写入与读取 ---")
    sio.write("test.csv", [["id", "value"], [1, "a"], [2, "b"]])
    # 如果安装了 pandas，这里会返回 DataFrame
    csv_content = sio.read("test.csv")
    print(f"CSV 内容类型: {type(csv_content)}")

    print("\n--- 测试大文件流式读取 ---")
    # 创建一个 20MB 的临时文本文件
    large_file = "large_text.txt"
    if not os.path.exists(large_file):
        with open(large_file, "w") as f:
            for i in range(2000000):
                f.write("This is a test line for streaming IO performance.\n")
    
    # 这里会自动选择流式读取（生成器），内存占用极低
    print("开始流式读取...")
    line_count = 0
    for line in sio.read(large_file):
        line_count += 1
    print(f"读取完成，共 {line_count} 行，内存安全。")
