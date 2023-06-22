# Demo PySpark Delta Table SCD2 implementation

[![Python package](https://github.com/spsoni/pyspark-delta-scd2/actions/workflows/python-package.yml/badge.svg)](https://github.com/spsoni/pyspark-delta-scd2/actions/workflows/python-package.yml)

This project utilizes `faker-pyspark` to generate random schema and dataframes to mimic data table snapshots.

Using these snapshots to process and apply SCD2 pattern into delta table as the destination. 

Source of Inspiration for SCD2 pattern: https://aws-blogs-artifacts-public.s3.amazonaws.com/artifacts/BDB-2547/glue/scd-deltalake-employee-etl-job.py 

## Installation

Install with pip:

``` bash
pip install pyspark-delta-scd2

```

### Generate incremental updates to dataframe and apply scd2

``` python
>>> from pyspark_delta_scd2 import get_spark, PySparkDeltaScd2
>>> spark = get_spark()
>>> demo  = PySparkDeltaScd2(spark=spark)
>>> # initial load
>>> df1   = demo.process()
>>> # incremental update
>>> df2   = demo.process()
>>> # df2 should have some deletes, updates and inserts

```
