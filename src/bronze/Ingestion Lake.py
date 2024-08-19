# Databricks notebook source
spark

# COMMAND ----------

catalog_name = "raw" 
schema_name = "transactions"
table_name = "transactions"
format_type = "csv"

# COMMAND ----------

def load_data(catalog_name, schema_name, table_name, format_type):
    """Loads data from source"""
    dataframe = spark.read.format(f"{format_type}")\
        .option("header", "true")\
        .load(f"/Volumes/{catalog_name}/{schema_name}/{table_name}/")
    
    return df

# COMMAND ----------

ingestion_data = load_data("raw", "transactions", "transactions", "csv")

# COMMAND ----------

# df = spark.read.format("csv")\
#     .option("header", "true")\
#     .load("/Volumes/raw/transactions/transactions/")
# df.show(4)

# COMMAND ----------

ingestion_data.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("bronze.transactions.transactions")
