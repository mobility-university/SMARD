import pandas as pd
import fastparquet
import glob
import os, shutil

from download_all import FILTERS


years = range(2015, 2022)


if __name__ == "__main__":
    dir = 'parquet_partitioned/'
    if os.path.isdir(dir):
        shutil.rmtree(dir)

    os.makedirs(dir)

    for year in years:
        for filter in FILTERS.keys():
            df = pd.read_csv(f'raw_data/{year}_{filter}.csv', names=["timestamp", "production"])
            df["year"] = year
            df["filter"] = filter
            df.to_parquet(f'parquet_partitioned/all_data.parquet', partition_cols=["year", "filter"])

    fastparquet.writer.merge(glob.glob("parquet_partitioned/all_data.parquet/year=*/filter=*/*.parquet"))
