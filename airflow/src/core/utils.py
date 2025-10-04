import gzip
import json
import os
from datetime import datetime
from typing import Callable, Tuple, Dict, List, Any, Union
from concurrent.futures import ThreadPoolExecutor, Future
from src.core.logger import console
from requests import Response
import brotli


def compress(results: list, store_path: str, ratio: int = 5):
    folder_name = f"flights_data_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}"
    path = os.path.join(store_path, folder_name)
    os.makedirs(path, exist_ok=True)
    with gzip.open(
        os.path.join(path, "data.json.gz"),
        "wt",
        encoding="utf-8",
        compresslevel=ratio,
    ) as f:
        json.dump(results, f, indent=2)


def generate_futures(
    func: Callable, args: List[Tuple[Any, ...]], max_workers: int = 4
) -> Dict[Future, Tuple[Any, ...]]:
    # generic function to create futures, returns dictionary {futures:args}
    futures: Dict[Future, Tuple[Any, ...]] = {}
    console.info(f"Creating futures for {func.__name__}")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for arg in args:
            future = pool.submit(func, *arg)
            futures[future] = arg

    return futures


def get_content(response: Response) -> Union[Dict, bytes]:
    __content_encodings = {
        "": lambda x: x,
        "br": brotli.decompress,
        "gzip": gzip.decompress,
    }

    content = response.content
    encoding = response.headers.get("Content-Encoding", "")
    content_type = response.headers["Content-Type"]

    print(content)
    print(encoding)
    print(content_type)

    try:
        content = __content_encodings[encoding](content)
    except Exception:
        pass

    if "application/json" in content_type:
        return json.loads(content)
