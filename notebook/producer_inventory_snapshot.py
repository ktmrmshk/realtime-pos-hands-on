# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC README:
# MAGIC 
# MAGIC `inventory_snapshot`を定期的にオブジェクトストレージ(DBFS)に配置するProducerスクリプト

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/generator/inventory_snapshot_*.txt')
)

display(df)
df.createOrReplaceTempView('inventory_snapshot')

# COMMAND ----------

# DBTITLE 1,(オプショナル)出力フォルダをクリアしておく
# MAGIC %fs rm -r /tmp/inventory_snapshot/

# COMMAND ----------

# MAGIC %run ./datagen

# COMMAND ----------

#from datagen import DataGen


dg = DataGen(
    spark_session=spark,
    tablename='inventory_snapshot', 
    dt_colname='date_time', 
    start_datetime=datetime.fromisoformat('2021-01-01'), 
    export_dir='/dbfs/tmp/inventory_snapshot'
    max_ite=5000,
    sleep_time=5,
    dt_step=timedelta(hours=1)
)
dg.start()
