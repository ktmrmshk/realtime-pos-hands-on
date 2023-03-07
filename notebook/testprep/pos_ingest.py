# Databricks notebook source
df_st = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('Header', True)
    .option('inferSchema', True)
    .option('cloudFiles.schemaLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshots.chkpoint')
    .load('/tmp/inventory_change_*.csv')
)

#df_st.awaitTermination()
#display(df_st)

(
    df_st.writeStream
    .format('delta')
    #.trigger(availableNow=True)
    .trigger(processingTime='2 seconds')
    .option('checkpointLocation', '/tmp/masahiko.kitamura@databricks.com/inventory_snapshots.chkpoint')
    .toTable('inventory_change_realtime')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC with inventory_change_realtime_hour AS (
# MAGIC   SELECT *, date_trunc('HOUR', date_time) as dte_hour
# MAGIC   from inventory_change_realtime
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

# MAGIC %sql
# MAGIC with recent_change AS (
# MAGIC   SELECT *, date_trunc('HOUR', date_time) as dte_hour, date_trunc('minute', date_time) as dte_min
# MAGIC   from inventory_change_realtime
# MAGIC   order by date_time desc
# MAGIC   limit 20000
# MAGIC )
# MAGIC select dte_hour, store_id, count(1) as count
# MAGIC  from recent_change
# MAGIC  group by dte_hour, store_id
# MAGIC order by dte_hour

# COMMAND ----------


