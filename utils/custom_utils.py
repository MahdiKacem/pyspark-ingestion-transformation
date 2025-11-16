from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, row_number, col, desc, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class Transformations:

    def dedup(self, df:DataFrame, dedup_cols:List,  cdc: str):
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col("dedupCounts")== 1)
        df = df.drop("dedupKey", "dedupCounts")
        return df
    
    def process_timestamp(self, df:DataFrame):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(self, df:DataFrame, key_cols, table:str, cdc:str):
        merge_condition = " AND ".join([f"t.{i} = src.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")
        dlt_obj.alias("t").merge(df.alias("src"), merge_condition)\
                          .whenMatchedUpdateAll(condition = f"src.{cdc} >= t.{cdc}")\
                          .whenNotMatchedInsertAll()\
                          .execute()
        return True