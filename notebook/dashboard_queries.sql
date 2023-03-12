-- Databricks notebook source
USE CATALOG main;
USE DATABASE mk1112; -- <== あなたの指定したユニーク名に書き換えてください

-- COMMAND ----------

with inventory_change_realtime_hour AS (
  SELECT *, date_trunc('HOUR', date_time) as dte_hour
  from inventory_change
)
SELECT 
  dte_hour, 
  store_id,
  count(1) as num_tx, 
  -sum(quantity) as num_sales 
FROM inventory_change_realtime_hour
where change_type_id = 1
group by dte_hour, store_id
order by dte_hour

-- COMMAND ----------

with recent_change AS (
  SELECT *, date_trunc('HOUR', date_time) as dte_hour, date_trunc('minute', date_time) as dte_min
  from inventory_change
  order by date_time desc
  limit 20000
)
select dte_hour, store_id, count(1) as count
 from recent_change
 group by dte_hour, store_id
order by dte_hour
