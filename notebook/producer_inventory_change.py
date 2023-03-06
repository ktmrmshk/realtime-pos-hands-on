# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC README:
# MAGIC 
# MAGIC `inventory_change`を定期的にオブジェクトストレージ(DBFS)に配置するProducerスクリプト

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/generator/inventory_change*.txt')
)
df.createOrReplaceTempView('inventory_change')

display(df)

# COMMAND ----------

# DBTITLE 1,(オプショナル)出力フォルダをクリアしておく
# MAGIC %fs rm -r /tmp/inventory_change/

# COMMAND ----------

# MAGIC %run ./datagen

# COMMAND ----------

dg = DataGen(
    spark_session=spark,
    tablename='inventory_change', 
    dt_colname='date_time', 
    start_datetime=datetime.fromisoformat('2021-01-01'), 
    export_dir='/dbfs/tmp/inventory_change',
    max_ite=5000,
    sleep_time=5,
    dt_step=timedelta(hours=1)
)
dg.start()
