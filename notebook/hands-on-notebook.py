# Databricks notebook source
# MAGIC %md ## 静的なテーブルの準備

# COMMAND ----------

# MAGIC %md ### Storeテーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/static_data/store.txt')
)
df.createOrReplaceTempView('store')

display(df)

# COMMAND ----------

# MAGIC %md ### Itemテーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/static_data/item.txt')
)
df.createOrReplaceTempView('item')
display(df)

# COMMAND ----------

# MAGIC %md ### Inventory Change Type(在庫数の変更種類)テーブル

# COMMAND ----------

df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/static_data/inventory_change_type.txt')
)
df.createOrReplaceTempView('inventory_change_type')

display(df)

# COMMAND ----------

# MAGIC %md ### リアルタイム処理

# COMMAND ----------

# MAGIC %fs rm -r /tmp/

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS mk1112;
# MAGIC CREATE DATABASE IF NOT EXISTS mk1112;
# MAGIC USE mk1112;

# COMMAND ----------

# MAGIC %md ### Inventry Change

# COMMAND ----------

df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('Header', True)
    .option('inferSchema', True)
    .option('cloudFiles.schemaLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_change.chkpoint')
    .load('/tmp/inventory_change_*.csv')
)

(
    df_st.writeStream
    .format('delta')
    #.trigger(availableNow=True)
    .trigger(processingTime='2 seconds')
    .option('checkpointLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_change.chkpoint')
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
    .option('cloudFiles.schemaLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshot.chkpoint')
    .load('/tmp/inventory_snapshot_*.csv')
)

(
    df_st.writeStream
    .format('delta')
    #.trigger(availableNow=True)
    .trigger(processingTime='2 seconds')
    .option('checkpointLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshot.chkpoint')
    .toTable('inventory_snapshot')
)


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



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 以下はスキップ(Debug)

# COMMAND ----------

# MAGIC %md ### Inventry Spapshot

# COMMAND ----------

# df_st = (
#     spark.readStream
#     .format('cloudFiles')
#     .option('cloudFiles.format', 'csv')
#     .option('Header', True)
#     .option('inferSchema', True)
#     .option('cloudFiles.schemaLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshots.chkpoint')
#     .load('/mnt/pos/generator/inventory_snapshot*.txt')
# )

# #df_st.awaitTermination()
# #display(df_st)

# (
#     df_st.writeStream
#     .format('delta')
#     .trigger(availableNow=True)
#     .option('checkpointLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshots.chkpoint')
#     .toTable('inventry_snapshots')
# )


df = (
    spark.read
    .format('csv')
    .option('Header', True)
    .option('inferSchema', True)
    .load('/mnt/pos/generator/inventory_snapshot_*.txt')
)

display(df)
df.createOrReplaceTempView('inventry_snapshot')

print( df.count() )

df.createOrReplaceTempView('inv_snapshot')
display(
    spark.sql(
        '''
        SELECT date_time, count(item_id) FROM inv_snapshot GROUP BY date_time ORDER BY date_time
        '''
    )
)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md ### Inventry Change

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

# MAGIC %md ## GOLD Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC   a.store_id,
# MAGIC   a.item_id,
# MAGIC   FIRST(a.quantity) as snapshot_quantity,
# MAGIC   FIRST(a.date_time) as date_time
# MAGIC from inv_snapshot as a
# MAGIC group by a.store_id, a.item_id
# MAGIC order by date_time desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   x.store_id,
# MAGIC   x.item_id,
# MAGIC   x.date_time,
# MAGIC   x.quantity
# MAGIC from inventory_change x
# MAGIC inner join store y on x.store_id = y.store_id
# MAGIC inner join inventory_change_type z on x.change_type_id = z.change_type_id
# MAGIC where not( y.name = 'online' and z.change_type = 'bopis')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold AS
# MAGIC 
# MAGIC select 
# MAGIC   a.store_id,
# MAGIC   a.item_id,
# MAGIC   FIRST(a.quantity) as snapshot_quantity,
# MAGIC   coalesce(sum(b.quantity), 0) as change_quantity,
# MAGIC   first(a.quantity) + coalesce(sum(b.quantity), 0) as current_inventory,
# MAGIC   GREATEST( FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC from inv_snapshot as a
# MAGIC left outer join inventory_change b
# MAGIC on a.store_id = b.store_id 
# MAGIC and a.item_id = b.item_id
# MAGIC and a.date_time <= b.date_time
# MAGIC 
# MAGIC group by a.store_id, a.item_id
# MAGIC order by date_time desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold;

# COMMAND ----------


