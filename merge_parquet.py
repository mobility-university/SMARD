import pandas as pd
import fastparquet
import glob
import os, shutil

from download_all import FILTERS


years = range(2015, 2022)


if __name__ == "__main__":
    dir = 'parquet_data/'
    if os.path.isdir(dir):
        shutil.rmtree(dir)

    os.makedirs(dir+"all_data.parquet/")

    for year in years:
        for filter in FILTERS.keys():
            df = pd.read_csv(f'raw_data/{year}_{filter}.csv', names=["timestamp", "production"])
            df["year"] = year
            df["filter"] = filter
            df.to_parquet(f'parquet_data/all_data.parquet/part_{year}_{filter}.parquet')

    fastparquet.writer.merge(glob.glob("parquet_data/all_data.parquet/part_*.parquet"))
