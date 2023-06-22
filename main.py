from pyspark_delta_scd2 import (
    get_spark,
    PySparkDeltaScd2
)

spark = get_spark()
demo = PySparkDeltaScd2(spark=spark)
# initial load
df1 = demo.process()

# incremental update
df2 = demo.process()

# df2 should have some deletes, updates and inserts
