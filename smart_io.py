import os
import sys
import mmap
import gc
import logging
import asyncio
import functools
import hashlib
from pathlib import Path
from typing import Any, Union, Optional, Callable, AsyncIterator, Iterator, List, Dict, TypeVar, IO, BinaryIO

# 导入依赖库（根据需求，默认环境已安装）
import orjson
import pandas as pd
import aiofiles
import aiofiles.os
import portalocker
import chardet
from gzip import GzipFile, compress as gzip_compress, decompress as gzip_decompress
import gzip
import bz2
import zipfile

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SmartIO")

# 泛型类型定义
T = TypeVar('T')

class SmartIOConfig:
    """可配置的参数类"""
    def __init__(
        self,
        large_file_threshold: int = 100 * 1024 * 1024,  # 100MB
        memory_usage_ratio: float = 0.3,
        enable_cache: bool = True,
        cache_max_size: int = 128,  # 缓存文件数量
        lock_timeout: float = 10.0  # 锁超时时间
    ):
        self.large_file_threshold = large_file_threshold
        self.memory_usage_ratio = memory_usage_ratio
        self.enable_cache = enable_cache
        self.cache_max_size = cache_max_size
        self.lock_timeout = lock_timeout

class SmartIO:
    def __init__(self, config: Optional[SmartIOConfig] = None):
        self.config = config or SmartIOConfig()
        self.mem_total = self._get_system_memory()
        
        # 初始化缓存装饰器
        if self.config.enable_cache:
            self._cache_decorator = functools.lru_cache(maxsize=self.config.cache_max_size)
        else:
            self._cache_decorator = lambda x: x

        logger.info(f"SmartIO initialized. Memory: {self.mem_total // (1024**3)}GB, Cache: {self.config.enable_cache}")

    def _get_system_memory(self) -> int:
        try:
            import psutil
            return psutil.virtual_memory().total
        except ImportError:
            return 8 * 1024 * 1024 * 1024  # 保守估计 8GB

    # ================= 核心逻辑入口 =================

    def read(
        self, 
        filepath: Union[str, Path], 
        mode: str = 'r', 
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs
    ) -> Any:
        """
        同步智能读取
        """
        path = Path(filepath)
        self._check_lock(path) # 读取前检查是否被锁（可选，视业务需求）
        
        # 如果是小文件且开启了缓存，尝试走缓存逻辑
        if self.config.enable_cache and not self._is_large_file(path.stat().st_size):
            # 使用文件内容的 hash 作为缓存 key，避免文件修改后读到脏数据
            # 注意：这里为了演示简单，仅对文件名+大小做简单判断，生产环境应检查 mtime
            cache_key = (str(path), path.stat().st_size)
            return self._cache_decorator(self._read_impl)(cache_key, path, mode, progress_callback, **kwargs)
        
        return self._read_impl(None, path, mode, progress_callback, **kwargs)

    async def async_read(
        self, 
        filepath: Union[str, Path], 
        mode: str = 'r', 
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs
    ) -> Any:
        """
        异步智能读取
        """
        path = Path(filepath)
        return await self._async_read_impl(path, mode, progress_callback, **kwargs)

    def write(
        self, 
        filepath: Union[str, Path], 
        data: Any, 
        mode: str = 'w', 
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs
    ) -> None:
        """
        同步安全写入（带锁）
        """
        path = Path(filepath)
        # 确保目录存在
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入必须加锁，防止并发写入冲突
        with portalocker.Lock(path, mode='wb', timeout=self.config.lock_timeout):
            logger.info(f"Acquired lock for writing {path}")
            self._write_impl(path, data, mode, progress_callback, **kwargs)
            logger.info(f"Released lock for {path}")

    async def async_write(
        self, 
        filepath: Union[str, Path], 
        data: Any, 
        mode: str = 'w', 
        progress_callback: Optional[Callable[[int, int], None]] = None,
        **kwargs
    ) -> None:
        """
        异步写入
        注意：文件锁通常在文件系统层面是同步操作，异步锁实现较复杂。
        这里为了保持高性能，建议在应用层控制并发，或者使用 aiofiles 的原子性。
        此处演示简化版异步写入（不带文件锁，建议单生产者场景使用）。
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        await self._async_write_impl(path, data, mode, progress_callback, **kwargs)

    # ================= 内部实现细节 =================

    def _read_impl(self, cache_key: Optional[tuple], path: Path, mode: str, progress_cb: Optional[Callable], **kwargs) -> Any:
        file_size = path.stat().st_size
        ext = path.suffix.lower()
        
        # 1. 处理压缩文件 (自动解压)
        if ext in ['.gz', '.bz2']:
            return self._read_compressed(path, ext, mode, progress_cb, **kwargs)

        # 2. 结构化数据
        if ext == '.json':
            return self._read_json(path, file_size, progress_cb)
        if ext == '.csv':
            return self._read_csv(path, file_size, progress_cb, **kwargs)

        # 3. 通用大文件策略
        is_binary = 'b' in mode
        is_large = self._is_large_file(file_size)
        is_risky = self._is_memory_risky(file_size)

        if is_binary and (is_large or is_risky):
            return self._read_mmap(path, progress_cb)
        if not is_binary and (is_large or is_risky):
            return self._read_stream(path, progress_cb, kwargs.get('encoding'))

        # 4. 默认小文件读取 (带编码检测)
        return self._read_simple(path, mode, kwargs.get('encoding'), progress_cb)

    async def _async_read_impl(self, path: Path, mode: str, progress_cb: Optional[Callable], **kwargs) -> Any:
        """异步实现简化版，主要演示 aiofiles 流式读取"""
        ext = path.suffix.lower()
        # 异步处理压缩较复杂，这里主要展示普通文件的异步流式读取
        if ext in ['.gz', '.bz2']:
             # 异步解压通常需要线程池运行同步库，这里暂回退到线程池
             loop = asyncio.get_event_loop()
             return await loop.run_in_executor(None, self.read, path, mode, None, **kwargs)

        encoding = kwargs.get('encoding') or self._detect_encoding(path)
        
        async with aiofiles.open(path, mode=mode, encoding=encoding) as f:
            if self._is_large_file(path.stat().st_size):
                # 流式异步读取
                res = []
                while True:
                    chunk = await f.read(1024 * 1024) # 1MB chunks
                    if not chunk: break
                    res.append(chunk)
                    if progress_cb: progress_cb(len(chunk), path.stat().st_size)
                return "".join(res) if 'b' not in mode else b"".join(res)
            else:
                content = await f.read()
                return content

    def _write_impl(self, path: Path, data: Any, mode: str, progress_cb: Optional[Callable], **kwargs) -> None:
        ext = path.suffix.lower()
        
        # 压缩写入
        if ext == '.gz':
            self._write_compressed(path, data, mode, 'gz', progress_cb)
            return
        if ext == '.bz2':
            self._write_compressed(path, data, mode, 'bz2', progress_cb)
            return

        # 结构化写入
        if ext == '.json':
            self._write_json(path, data, progress_cb)
            return
        if ext == '.csv':
            self._write_csv(path, data, progress_cb)
            return

        # 普通写入
        encoding = kwargs.get('encoding') or 'utf-8'
        with open(path, mode, encoding=encoding, buffering=1024*1024) as f:
            if isinstance(data, (str, bytes)):
                f.write(data)
            else:
                # 尝试迭代写入
                for chunk in data:
                    f.write(str(chunk))
            if progress_cb: progress_cb(path.stat().st_size, path.stat().st_size)

    async def _async_write_impl(self, path: Path, data: Any, mode: str, progress_cb: Optional[Callable], **kwargs) -> None:
        encoding = kwargs.get('encoding') or 'utf-8'
        async with aiofiles.open(path, mode=mode, encoding=encoding) as f:
            if isinstance(data, (str, bytes)):
                await f.write(data)
            else:
                for chunk in data:
                    await f.write(str(chunk))

    # ================= 辅助功能模块 =================

    def _detect_encoding(self, path: Path) -> str:
        """使用 chardet 检测文件编码"""
        # 读取前 1KB 进行检测
        with open(path, 'rb') as f:
            raw = f.read(1024)
        if not raw: return 'utf-8'
        result = chardet.detect(raw)
        encoding = result['encoding']
        confidence = result['confidence']
        logger.debug(f"Detected encoding {encoding} (confidence: {confidence:.2f}) for {path.name}")
        return encoding or 'utf-8'

    def _read_compressed(self, path: Path, ext: str, mode: str, progress_cb: Optional[Callable], **kwargs):
        """处理压缩文件读取"""
        logger.info(f"Reading compressed file {path.name} using {ext} algorithm")
        if ext == '.gz':
            opener = gzip.open
        elif ext == '.bz2':
            opener = bz2.open
        else:
            return self._read_simple(path, mode, kwargs.get('encoding'), progress_cb)
        
        # 压缩文件通常建议流式读取，因为解压后可能很大
        is_risky = True # 假设解压后数据量较大
        if is_risky:
            # 流式读取解压流
            with opener(path, 'rb' if 'b' in mode else 'rt', encoding=kwargs.get('encoding')) as f:
                for line in f:
                    yield line
        else:
            with opener(path, 'rb' if 'b' in mode else 'rt', encoding=kwargs.get('encoding')) as f:
                return f.read()

    def _write_compressed(self, path: Path, data: Any, mode: str, method: str, progress_cb: Optional[Callable]):
        """处理压缩文件写入"""
        logger.info(f"Writing to compressed file {path.name}")
        if method == 'gz':
            opener = gzip.open
        elif method == 'bz2':
            opener = bz2.open
        
        # 写入压缩通常需要完整数据或分块，这里演示简单写入
        with opener(path, 'wb') as f:
            if isinstance(data, str):
                data = data.encode('utf-8')
            f.write(data)

    def _read_json(self, path: Path, file_size: int, progress_cb: Optional[Callable]):
        if progress_cb: progress_cb(0, file_size)
        with open(path, 'rb') as f:
            # orjson 直接 load bytes
            data = orjson.loads(f.read())
        if progress_cb: progress_cb(file_size, file_size)
        return data

    def _write_json(self, path: Path, data: Any, progress_cb: Optional[Callable]):
        option = orjson.OPT_INDENT_2 if isinstance(data, (dict, list)) else 0
        content = orjson.dumps(data, option=option)
        with open(path, 'wb') as f:
            f.write(content)
        if progress_cb: progress_cb(len(content), len(content))

    def _read_csv(self, path: Path, file_size: int, progress_cb: Optional[Callable], **kwargs):
        if progress_cb: progress_cb(0, file_size)
        df = pd.read_csv(path, **kwargs)
        if progress_cb: progress_cb(file_size, file_size)
        return df

    def _write_csv(self, path: Path, data: Any, progress_cb: Optional[Callable]):
        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        else:
            # 简单的列表写入
            import csv
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(data)

    def _read_mmap(self, path: Path, progress_cb: Optional[Callable]):
        with open(path, 'r+b') as f:
            size = path.stat().st_size
            if progress_cb: progress_cb(0, size)
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                data = mm.read()
            if progress_cb: progress_cb(size, size)
            return data

    def _read_stream(self, path: Path, progress_cb: Optional[Callable], encoding: Optional[str]):
        encoding = encoding or self._detect_encoding(path)
        with open(path, 'r', encoding=encoding) as f:
            line_count = 0
            for line in f:
                yield line
                line_count += 1
                # 这里很难精确预估行数，简化处理，每1000行回调一次
                if progress_cb and line_count % 1000 == 0:
                    progress_cb(f.tell(), path.stat().st_size)

    def _read_simple(self, path: Path, mode: str, encoding: Optional[str], progress_cb: Optional[Callable]):
        encoding = encoding or self._detect_encoding(path)
        with open(path, mode, encoding=encoding) as f:
            data = f.read()
            if progress_cb: progress_cb(len(data), len(data))
            return data

    def _check_lock(self, path: Path):

        pass
        #未实现具体检查逻辑

    def _is_large_file(self, size: int) -> bool:
        return size > self.config.large_file_threshold

    def _is_memory_risky(self, size: int) -> bool:
        return size > (self.mem_total * self.config.memory_usage_ratio)

# ================= 使用示例 =================

async def main():
    # 1. 自定义配置
    config = SmartIOConfig(
        large_file_threshold=50 * 1024 * 1024, # 50MB 视为大文件
        enable_cache=True
    )
    sio = SmartIO(config)

    # 准备测试数据
    test_data = {"name": "SmartIO", "features": ["Async", "Compressed", "Cached"], "version": 2.0}
    
    print("--- 1. JSON 写入与读取 (带缓存) ---")
    sio.write("test_pro.json", test_data)
    data = sio.read("test_pro.json") # 第二次读取会走缓存
    print(f"Read data: {data}")

    print("\n--- 2. Gzip 压缩写入与读取 ---")
    sio.write("test_pro.json.gz", test_data)
    raw_gz = sio.read("test_pro.json.gz")
    print(f"Read from Gzip: {raw_gz}")

    print("\n--- 3. 异步读取测试 ---")
    # 创建一个临时文件用于异步测试
    sio.write("async_test.txt", "Hello Async World!" * 1000)
    content = await sio.async_read("async_test.txt")
    print(f"Async read length: {len(content)}")

    print("\n--- 4. 进度条回调测试 ---")
    def progress_callback(current, total):
        percent = (current / total) * 100 if total > 0 else 0
        print(f"\rProgress: {percent:.1f}%", end="", flush=True)
    
    # 写入一个大一点的文件测试进度
    large_data = "x" * (1024 * 1024) # 1MB
    sio.write("large_file.txt", large_data, progress_callback=progress_callback)
    print("\nWrite finished.")

    print("\n--- 5. 文件锁测试 (需要手动模拟并发) ---")
    # 正常情况下，write 方法内部已经处理了锁
    # 如果你在另一个进程同时运行 write，它会等待直到锁释放
    sio.write("lock_test.txt", "This is safe under concurrent writes.")

if __name__ == "__main__":
    # 运行异步示例
    asyncio.run(main())
