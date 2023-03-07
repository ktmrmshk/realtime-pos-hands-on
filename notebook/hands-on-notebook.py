# Databricks notebook source
# MAGIC %md ## 静的なテーブルの準備

# COMMAND ----------

#UNIQUE_NAME = 'YOUR_UNIQUE_NAME'
UNIQUE_NAME = 'mk1112' # <==ここを置き換えてください

print(f'Your database name => {UNIQUE_NAME}')

spark.sql(f'DROP DATABASE IF EXISTS {UNIQUE_NAME} CASCADE;')
spark.sql(f'CREATE DATABASE IF NOT EXISTS {UNIQUE_NAME};')
spark.sql(f'USE {UNIQUE_NAME};')

# COMMAND ----------

# MAGIC %md ### Storeテーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('dbfs:/tmp/realtime_pos/dest/store.txt')
)

# DeltaLakeテーブルに書き出す
df.write.format('delta').mode('overwrite').saveAsTable('store')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM store;

# COMMAND ----------

# MAGIC %md ### Itemテーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('dbfs:/tmp/realtime_pos/dest/item.txt')
)

df.write.format('delta').mode('overwrite').saveAsTable('item')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM item;

# COMMAND ----------

# MAGIC %md ### Inventory Change Type(在庫数の変更種類)テーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('dbfs:/tmp/realtime_pos/dest/inventory_change_type.txt')
)

df.write.format('delta').mode('overwrite').saveAsTable('inventory_change_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_change_type

# COMMAND ----------

# MAGIC %md ### リアルタイム処理

# COMMAND ----------

dbutils.fs.rm(f'/tmp/realtime_pos/inventory_change/', True)
dbutils.fs.rm(f'/tmp/realtime_pos/inventory_snapshot/', True)
dbutils.fs.rm(f'/tmp/{UNIQUE_NAME}/', True)

# COMMAND ----------

# MAGIC %md ### Inventry Change

# COMMAND ----------

df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('Header', True)
    .option('inferSchema', True)
    .option('cloudFiles.schemaLocation', '/tmp/{UNIQUE_NAME}/inventory_change.chkpoint')
    .load('/tmp/realtime_pos/inventory_change/inventory_change_*.csv')
)

(
    df_st.writeStream
    .format('delta')
    #.trigger(availableNow=True)
    .trigger(processingTime='2 seconds')
    .option('checkpointLocation', '/tmp/{UNIQUE_NAME}/inventory_change.chkpoint')
    .toTable('inventory_change')
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_change
# MAGIC order by date_time desc limit 50

# COMMAND ----------

# MAGIC %md ### Inventry Spapshot

# COMMAND ----------

df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('Header', True)
    .option('inferSchema', True)
    .option('cloudFiles.schemaLocation', '/tmp/{UNIQUE_NAME}/inventory_snapshot.chkpoint')
    .load('/tmp/realtime_pos/inventory_snapshot/inventory_snapshot_*.csv')
)

(
    df_st.writeStream
    .format('delta')
    #.trigger(availableNow=True)
    .trigger(processingTime='2 seconds')
    .option('checkpointLocation', '/tmp/{UNIQUE_NAME}/inventory_snapshot.chkpoint')
    .toTable('inventory_snapshot')
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_snapshot
# MAGIC order by date_time desc limit 50

# COMMAND ----------

# MAGIC %md ### 最新の在庫テーブル(Gold)テーブル

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW gold AS
# MAGIC select
# MAGIC   a.store_id,
# MAGIC   a.item_id,
# MAGIC   FIRST(a.quantity) as snapshot_quantity,
# MAGIC   coalesce(sum(b.quantity), 0) as change_quantity,
# MAGIC   first(a.quantity) + coalesce(sum(b.quantity), 0) as current_inventory,
# MAGIC   GREATEST(FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC from
# MAGIC   inventory_snapshot as a
# MAGIC   left outer join inventory_change b on a.store_id = b.store_id
# MAGIC   and a.item_id = b.item_id
# MAGIC   and a.date_time <= b.date_time
# MAGIC group by
# MAGIC   a.store_id,
# MAGIC   a.item_id
# MAGIC order by
# MAGIC   date_time desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold;

# COMMAND ----------


