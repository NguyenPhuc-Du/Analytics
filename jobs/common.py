# jobs/common.py
import logging
import os
import sys
import time


def setup_logger(name, log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
    ):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.perf_counter() - self.start, 2)


def wait_for_executors(sc, timeout=60):
    count = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        if count > 0:
            time.sleep(3)
            return sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        time.sleep(1)
    return count


def write_parquet(df, path, single_file=True):
    writer = df.coalesce(1) if single_file else df
    writer.write.mode("overwrite").parquet(path)
    return path


def dir_size_mb(path):
    if not os.path.exists(path):
        return 0.0
    if os.path.isfile(path):
        return round(os.path.getsize(path) / 1024.0 / 1024.0, 2)
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return round(total / 1024.0 / 1024.0, 2)


def fmt(n):
    if n is None:
        return "*NULL*"
    if isinstance(n, int):
        return "{:,}".format(n)
    return str(n)