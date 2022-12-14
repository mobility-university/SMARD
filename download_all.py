#!/usr/bin/env python3
from datetime import datetime
import shutil
import os
import requests
from joblib import Parallel, delayed

from config import FILTERS


def get_timestamps(filter):
    r = requests.get(
        f"https://smard.api.proxy.bund.dev/app/chart_data/{filter}/DE/index_hour.json",
        headers={
            "accept": "application/json",
        }
    )
    assert r.ok
    return r.json()["timestamps"]


def get_data(filter, timestamp):
    data = requests.get(
        f"https://smard.api.proxy.bund.dev/app/chart_data/{filter}/DE/{filter}_DE_hour_{timestamp}.json",
        headers={
            "accept": "application/json",
        }
    )
    assert data.ok
    return data.json()["series"]


def download(filter):
    file_handles = {}

    timestamps = get_timestamps(filter)
    for timestamp in timestamps:
        data = get_data(filter, timestamp)
        for time_in_nanoseconds, power in data:
            year = datetime.fromtimestamp(time_in_nanoseconds/1000.).year
            path = f"raw_data/{year}_{filter}.csv"
            if path not in file_handles:
                file_handles[path] = open(path, "a")
            file_handles[path].write(f"{time_in_nanoseconds},{power}\n")

    for handle in file_handles.values():
        handle.close()

if __name__ == "__main__":
    dir = 'raw_data/'
    if os.path.isdir(dir):
        shutil.rmtree(dir)

    os.makedirs(dir)

    Parallel(n_jobs=20)(delayed(download)(filter) for filter in FILTERS.keys())
