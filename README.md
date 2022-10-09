# SMARD - Analysis of German electricity market data 

The German Bundesnetzagentur is providing data on the German electricity market. In this project, we want to download and analyze this data.

Bundesnetzagentur Download-Center: https://www.smard.de/home/downloadcenter/download-marktdaten
SMARD API: https://smard.api.bund.dev


## Setup Conda-Environment

* conda create -n smard python=3.8
* conda activate smard
* pip install -r requirements.txt


## Download data and store as parquet

Run `python download_all.py` to download data and `python merge_parquet.py` or `python merge_parquet_partitioned.py` to merge the data into one parquet-file. The latter will create a partitioned parquet file (which is at the moment not supported by DuckDB but it is supported by Spark).

## Analysis with Spark

Use the `spark-starter.ipynb` for analysis of the data with Apache Spark.
