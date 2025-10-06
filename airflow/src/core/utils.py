import gzip
import json
import os
from datetime import datetime
from typing import Callable, Tuple, Dict, List, Any, Union
from concurrent.futures import ThreadPoolExecutor, Future
from src.core.logger import console
from requests import Response
import brotli


def compress(results: list, store_path: str, file_prefix: str, ratio: int = 5):
    folder_name = f"{file_prefix}_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}"
    path = os.path.join(store_path, folder_name)
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "data.json.gz")
    with gzip.open(
        filepath,
        "wt",
        encoding="utf-8",
        compresslevel=ratio,
    ) as f:
        json.dump(results, f, indent=2)

    return filepath


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

    try:
        content = __content_encodings[encoding](content)
    except Exception:
        pass

    if "application/json" in content_type:
        return json.loads(content)


def pull_from_xcom(task_ids, key, **context):
    # This can be moved to utils or core as it is generalized function.
    value = context["ti"].xcom_pull(task_ids=task_ids, key=key)
    return value
