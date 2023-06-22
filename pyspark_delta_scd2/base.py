import os
from faker import Faker
from faker_pyspark import PySparkProvider
from pyspark_delta_scd2.utils import *


class PySparkDeltaScd2:
    def __init__(
            self,
            spark: SparkSession = None,
            with_deletes: bool = True,
            staging_table_path: str = None,
            target_table_path: str = None):
        self.fake = Faker(locale='en_AU')
        Faker.seed()
        self.fake.add_provider(PySparkProvider)
        uuid = self.fake.uuid4()

        # pass parameters to pyspark_schema as per your needs
        self.schema = self.fake.pyspark_schema()

        self.spark = get_spark() if spark is None else spark
        self.with_deletes = with_deletes
        self.staging_table_path = staging_table_path or f'/tmp/delta_staging_{uuid}'
        self.target_table_path = target_table_path or f'/tmp/delta_target_{uuid}'

    def generate_df(self, rows: int = 4):
        return self.fake.pyspark_dataframe(rows=rows, schema=self.schema)

    def process(self, src_df: DataFrame = None, generate_updates: bool = True):
        src_df = self.generate_df() if src_df is None else src_df

        if os.path.exists(self.target_table_path):
            # table pre-exist

            # read delta lake target table for current records
            target_tbl = DeltaTable.forPath(self.spark, self.target_table_path)
            target_tbl_df = target_tbl.toDF()
            target_tbl_df = target_tbl_df.drop('start_date', 'end_date', 'is_active', 'hash_key')

            if self.with_deletes:
                target_tbl_df = target_tbl_df.drop('delete_flag')

            if generate_updates:
                updated_dst_df = self.fake.pyspark_update_dataframe(target_tbl_df)
                src_df = src_df.union(updated_dst_df)

            src_df = add_row_checksum(src_df)

            stage_tbl = create_stage_deltalake_tbl(src_df, self.staging_table_path)

            target_tbl_df = target_tbl.toDF().where(col('is_active') == "Y")

            if self.with_deletes:
                target_tbl_df = target_tbl_df.where(col('delete_flag') == "N")

            union_updates_dels = capture_changes(stage_tbl, target_tbl_df, self.with_deletes)

            if self.with_deletes:
                # perform soft deletes
                delta_apply_soft_deletes(target_tbl, union_updates_dels)

            # refresh target table and perform merge operation
            target_tbl = DeltaTable.forPath(self.spark, self.target_table_path)

            upsert_records(target_tbl, union_updates_dels, with_deletes=self.with_deletes)
        else:
            # create new
            print(f'Delta lake destination data doesnt exists. Performing initial data load ...')

            src_df = add_row_checksum(src_df)
            src_df = (
                src_df
                .withColumn("start_date", current_timestamp())
                .withColumn("end_date", lit(None).cast(StringType()))
                .withColumn("is_active", lit("Y").cast(StringType()))
            )

            if self.with_deletes:
                src_df = src_df.withColumn("delete_flag", lit("N").cast(StringType()))

            create_deltalake_tbl(src_df, self.target_table_path)

        # refresh target table and return
        return DeltaTable.forPath(self.spark, self.target_table_path).toDF()
