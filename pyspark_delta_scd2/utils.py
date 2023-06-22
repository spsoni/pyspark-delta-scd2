from copy import copy
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable, DataFrame, SparkSession

from pyspark.sql.functions import (
    col,
    StringType,
    concat_ws,
    sha2,
    lit,
    current_timestamp
)

HASH_KEY_COLUMN_NAME = 'hash_key'


def add_row_checksum(
        df: DataFrame,
        column_name: str = HASH_KEY_COLUMN_NAME,
        using_columns: list = None,
        metadata_bool_key: str = 'hash_it'):
    """
    This function will generate hash key using sha2 and adds new column hash_key to dataframe.
    """
    if not using_columns:
        using_columns = list()
        schema = df.schema
        for field in schema.fields:
            if field.metadata and field.metadata.get(metadata_bool_key):
                using_columns.append(
                    col(field.name).cast(StringType())
                )

    df = df.withColumn(column_name, sha2(concat_ws("||", *using_columns), 256))

    # lazy way to add metadata, please fix it
    for field in df.schema.fields:
        if field.name == column_name:
            field.metadata = {metadata_bool_key: False}
    return df


def create_deltalake_tbl(df: DataFrame, path: str):
    """
    This function will write dataframe to deltalake format.
    """
    df.write.format("delta").mode("append").save(path)


def create_stage_deltalake_tbl(df: DataFrame, path: str):
    """
    This function will overwrites existing data in s3 prefix and write dataframe to S3 in deltalake format.
    """
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
    spark_ = df.sparkSession
    return DeltaTable.forPath(spark_, path)


def capture_changes(stage_tbl: DeltaTable, target_tbl_df: DataFrame, with_deletes: bool = False):
    """
    This function will identify changes between source data and target deltalake dataset.

    """
    cols = copy(target_tbl_df.columns)
    cols.remove('start_date')
    cols.remove('end_date')
    cols.remove('is_active')

    # join target and stage tables to identify updates
    updates_df = (
        stage_tbl
        .toDF()
        .alias("stage")
        .join(
            target_tbl_df.alias("target"), "uid", "left"
        )
        .filter(
            (col('target.hash_key').isNull()) | (col('target.hash_key') != col('stage.hash_key'))
        )
    )

    stage_updates = updates_df.select('stage.*')
    if with_deletes:
        stage_updates = stage_updates.withColumn("delete_flag", lit('N').cast(StringType()))
    stage_updates = stage_updates.select(*cols)

    target_updates = (
        updates_df
        .select('target.*')
        .drop('is_active', 'start_date', 'end_date')
        .select(*cols)
    )

    # perform union
    union_updates = (
        target_updates
        .union(stage_updates)
        .filter(
            col('hash_key').isNotNull()
        )
    )

    if with_deletes:
        # identify deleted records
        drop_del_cols = ['delete_flag', 'start_date', 'end_date', 'is_active']

        del_df = (
            target_tbl_df
            .alias('target')
            .join(
                stage_tbl
                .toDF()
                .alias('stage'),
                'uid',
                'anti'
            )
            .drop(*drop_del_cols)
            .withColumn("merge_delete_flag", lit("Y").cast(StringType()))
            .withColumnRenamed('merge_delete_flag', 'delete_flag')
        )
        union_updates = union_updates.union(del_df)

    return union_updates


def delta_apply_soft_deletes(target_tbl, union_updates_dels):
    """
    This function will identify delete records and sets delete_flag to true and updates end_date.
    """
    delete_join_cond = "target.uid=updates.uid and target.hash_key = updates.hash_key"
    delete_cond = "target.hash_key = updates.hash_key and target.is_active = 'Y' and updates.delete_flag = 'Y'"

    (
        target_tbl
        .alias("target")
        .merge(
            union_updates_dels
            .alias("updates"),
            delete_join_cond
        ).whenMatchedUpdate(
            condition=delete_cond,
            set={
                "is_active": lit("N").cast(StringType()),
                "end_date": current_timestamp(),
                "delete_flag": lit("Y").cast(StringType())
            }
        )
        .execute()
    )


def upsert_records(target_tbl: DeltaTable, union_updates_dels: DataFrame, with_deletes: bool = False):
    """
    This function will identify insert/update records and performs merge operation on exsiting deltalake dataset.
    """
    upsert_join_cond = "target.uid=updates.uid and target.hash_key = updates.hash_key and target.is_active = 'Y'"
    if with_deletes:
        upsert_update_cond = "target.is_active = 'Y' and updates.delete_flag = 'N'"
    else:
        upsert_update_cond = "target.is_active = 'Y'"

    update_cols = copy(list(union_updates_dels.columns))
    update_params = dict()
    for update_col in update_cols:
        update_params[update_col] = f'updates.{update_col}'

    (
        target_tbl
        .alias("target")
        .merge(
            union_updates_dels
            .alias("updates"),
            upsert_join_cond
        )
        .whenMatchedUpdate(
            condition=upsert_update_cond,
            set={
                "is_active": lit("N").cast(StringType()),
                "end_date": current_timestamp()
            }
        )
        .whenNotMatchedInsert(
            values={
                "is_active": lit("Y").cast(StringType()),
                "start_date": current_timestamp(),
                "end_date": "null",
                **update_params
            }
        )
        .execute()
    )


def get_spark(app_name:str = 'pyspark_delta_scd2') -> SparkSession:
    return (
        configure_spark_with_delta_pip(
            SparkSession.builder.appName(app_name)
            .config(
                'spark.sql.extensions',
                'io.delta.sql.DeltaSparkSessionExtension'
            )
            .config(
                'spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog'
            )
        ).getOrCreate()
    )
