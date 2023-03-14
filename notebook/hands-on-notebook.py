# Databricks notebook source
# MAGIC %md ## 静的なテーブルの準備

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;

# COMMAND ----------

#UNIQUE_NAME = 'YOUR_UNIQUE_NAME'
UNIQUE_NAME = 'mk1112' # <==ここを置き換えてください

print(f'Your database name => {UNIQUE_NAME}')

# データベースの初期化(サラの状態にする)
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

# MAGIC %md ## 動的なテーブル(リアルタイム処理)

# COMMAND ----------

dbutils.fs.rm(f'/tmp/{UNIQUE_NAME}/', True)

# COMMAND ----------

# MAGIC %md ### Inventory Change

# COMMAND ----------

inventory_change_schema = '''
  trans_id string,
  item_id string,
  store_id int,
  date_time timestamp,
  quantity int,
  change_type_id int
'''


df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('Header', True)
    .option('cloudFiles.schemaLocation', f'/tmp/{UNIQUE_NAME}/inventory_change.chkpoint')
    .schema(inventory_change_schema)
    .load('/tmp/realtime_pos/inventory_change/inventory_change_*.json')
)

(
    df_st.writeStream
    .format('delta')
    .trigger(availableNow=True)
    #.trigger(processingTime='2 seconds')
    .option('checkpointLocation', f'/tmp/{UNIQUE_NAME}/inventory_change.chkpoint')
    .toTable('inventory_change')
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_change
# MAGIC order by date_time desc limit 50

# COMMAND ----------

# MAGIC %md ### Inventory Spapshot

# COMMAND ----------

inventory_snapshot_schema = '''
item_id int,
employee_id int,
store_id int,
date_time timestamp,
quantity int
'''


df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('Header', True)
    .option('cloudFiles.schemaLocation', f'/tmp/{UNIQUE_NAME}/inventory_snapshot.chkpoint')
    .schema(inventory_snapshot_schema)
    .load('/tmp/realtime_pos/inventory_snapshot/inventory_snapshot_*.csv')
)

(
    df_st.writeStream
    .format('delta')
    .trigger(availableNow=True)
    #.trigger(processingTime='2 seconds')
    .option('checkpointLocation', f'/tmp/{UNIQUE_NAME}/inventory_snapshot.chkpoint')
    .toTable('inventory_snapshot')
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inventory_snapshot
# MAGIC order by date_time desc limit 50

# COMMAND ----------

# MAGIC %md ## ビジネスサマリテーブル(マート)

# COMMAND ----------

# MAGIC %md ### 最新の在庫テーブル(Gold)テーブル (簡易版)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW current_stock_summary_simple AS
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
# MAGIC select * from current_stock_summary_simple;

# COMMAND ----------

# MAGIC %md ### 最新の在庫テーブル(Gold)テーブル (詳細フィルター版)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW current_stock_summary AS
# MAGIC SELECT
# MAGIC   -- calculate current inventory
# MAGIC   a.store_id,
# MAGIC   a.item_id,
# MAGIC   FIRST(a.quantity) as snapshot_quantity,
# MAGIC   COALESCE(SUM(b.quantity), 0) as change_quantity,
# MAGIC   FIRST(a.quantity) + COALESCE(SUM(b.quantity), 0) as current_inventory,
# MAGIC   GREATEST(FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC FROM
# MAGIC   inventory_snapshot a -- access latest snapshot
# MAGIC   LEFT OUTER JOIN (
# MAGIC     -- calculate inventory change with bopis corrections
# MAGIC     SELECT
# MAGIC       x.store_id,
# MAGIC       x.item_id,
# MAGIC       x.date_time,
# MAGIC       x.quantity
# MAGIC     FROM
# MAGIC       inventory_change x
# MAGIC       INNER JOIN store y ON x.store_id = y.store_id
# MAGIC       INNER JOIN inventory_change_type z ON x.change_type_id = z.change_type_id
# MAGIC     WHERE
# MAGIC       NOT(
# MAGIC         y.name = 'online'
# MAGIC         AND z.change_type = 'bopis'
# MAGIC       ) -- exclude bopis records from online store
# MAGIC   ) b ON a.store_id = b.store_id
# MAGIC   AND a.item_id = b.item_id
# MAGIC   AND a.date_time <= b.date_time
# MAGIC GROUP BY
# MAGIC   a.store_id,
# MAGIC   a.item_id
# MAGIC ORDER BY
# MAGIC   date_time DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from current_stock_summary

# COMMAND ----------

# MAGIC %md ## ダッシュボード用クエリ
# MAGIC 
# MAGIC 以下のセルのクエリを「SQLモード」のSQLエディタにコピー%ペーストして使用してください。

# COMMAND ----------

# DBTITLE 1,売上の時間推移(全体)/トランザクション数
# MAGIC %sql
# MAGIC 
# MAGIC -- 売上の時間推移
# MAGIC with inventory_change_realtime_hour AS (
# MAGIC   SELECT *, date_trunc('HOUR', date_time) as dte_hour
# MAGIC   from inventory_change
# MAGIC   order by date_time desc
# MAGIC   limit 100000
# MAGIC )
# MAGIC SELECT 
# MAGIC   dte_hour, 
# MAGIC   store_id,
# MAGIC   count(1) as num_tx, 
# MAGIC   -sum(quantity) as num_sales 
# MAGIC FROM inventory_change_realtime_hour
# MAGIC where change_type_id = 1
# MAGIC group by dte_hour, store_id
# MAGIC order by dte_hour

# COMMAND ----------

# DBTITLE 1,月間の来客数
# MAGIC %sql
# MAGIC 
# MAGIC SELECT count( distinct trans_id ) from inventory_change
