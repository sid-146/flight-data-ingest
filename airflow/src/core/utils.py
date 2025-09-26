import gzip
import json
import os
from datetime import datetime
from typing import Callable, Tuple, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, Future


def compress(results: list, store_path: str, ratio: int = 5):
    folder_name = f"flights_data_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}"
    path = os.path.join(store_path, folder_name)
    with gzip.open(
        path,
        "wt",
        encoding="utf-8",
        compresslevel=ratio,
    ) as f:
        json.dump(results, f, indent=2)


def generate_futures(
    func: Callable, *args: List[Tuple[Any, ...]], max_workers: int = 4
) -> Dict[Future, Tuple[Any, ...]]:
    # generic function to create futures, returns dictionary {futures:args}
    futures: Dict[Future, Tuple[Any, ...]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for arg in args:
            future = pool.submit(func, *arg)
            futures[future] = arg

    return futures
