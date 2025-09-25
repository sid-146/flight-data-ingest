import gzip
import os
import json
from datetime import datetime


def compress(results: list, store_path: str, ratio: int = 5):
    folder_name = f"flights_data_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}"
    path = (os.path.join(store_path, folder_name),)
    with gzip.open(
        path,
        "wt",
        encoding="utf-8",
        compresslevel=ratio,
    ) as f:
        json.dump(results, f, indent=2)
