# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json

# COMMAND ----------

catalog_name = "raw" 
schema_name = "transactions"
table_name = "transactions"
format_type = "csv"

# COMMAND ----------

schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("UserID", StringType(), True),
    StructField("TransactionDate", StringType(), True),
    StructField("TransactionValue", StringType(), True),
    StructField("TransactionType", StringType(), True)
])

# COMMAND ----------

def load_data(catalog_name, schema_name, table_name, format_type, schema):
    """Loads data from source"""
    dataframe = spark.read.format(f"{format_type}")\
        .option("header", "true")\
        .schema(schema)\
        .load(f"/Volumes/{catalog_name}/{schema_name}/{table_name}/")
    
    return dataframe

# COMMAND ----------

# ingestion_data = load_data("raw", "transactions", "transactions", "csv", schema)

# COMMAND ----------

# %sql
# CREATE VOLUME IF NOT EXISTS "raw"."transactions"."transactions_checkpoint" 
# LOCATION "s3:/Volumes/raw/transactions/transactions_checkpoint/"

# %sql
# CREATE VOLUME IF NOT EXISTS raw.transactions.transactions_checkpoint
# LOCATION 's3://Volumes/raw/transactions/transactions_checkpoint/'

# COMMAND ----------

df_stream = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("header", "true")
                  .schema(schema)
                  .load(f"/Volumes/raw/transactions/transactions/")
)

def write_to_bronze_table(df, batchId):
    df.write.format("delta")\
        .mode("append")\
        .saveAsTable("bronze.transactions.transactions")

stream = (df_stream.writeStream
                   .option("checkpointLocation", f"/Volumes/raw/transactions/transactions_checkpoint/")
                   .foreachBatch(write_to_bronze_table)
                   .trigger(availableNow=True)
          )

# COMMAND ----------

start = stream.start()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE FROM bronze.transactions.transactions;
# MAGIC -- SELECT * FROM bronze.transactions.transactions
# MAGIC SELECT TransactionDate, count(*) FROM bronze.transactions.transactions GROUP BY TransactionDate ORDER BY TransactionDate DESC
# MAGIC

# COMMAND ----------

# Criar uma funcao para ler tabelas que ja existem no catalogo
# Se tabela nao existir, cria tabela
# Se tabela existir, nao cria
