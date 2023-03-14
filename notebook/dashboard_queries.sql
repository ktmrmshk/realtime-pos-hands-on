-- Databricks notebook source
USE CATALOG main;
USE DATABASE mk1112; -- <== あなたの指定したユニーク名に書き換えてください

-- COMMAND ----------

-- DBTITLE 1,売上の時間推移(全体)
-- 売上の時間推移
with inventory_change_realtime_hour AS (
  SELECT *, date_trunc('HOUR', date_time) as dte_hour
  from inventory_change
  order by date_time desc
  limit 100000
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

-- DBTITLE 1,トランザクション数
-- 売上の時間推移
with inventory_change_realtime_hour AS (
  SELECT *, date_trunc('HOUR', date_time) as dte_hour
  from inventory_change
  order by date_time desc
  limit 100000
)
SELECT 
  dte_hour, 
  store_id,
  count(1) as num_tx
FROM inventory_change_realtime_hour
where change_type_id = 1
group by dte_hour, store_id
order by dte_hour

-- COMMAND ----------

-- DBTITLE 1,月間の来客数
SELECT count( distinct trans_id ) from inventory_change
