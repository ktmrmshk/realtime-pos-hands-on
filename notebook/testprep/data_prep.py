# Databricks notebook source
# MAGIC %sh
# MAGIC cd /tmp/
# MAGIC curl -O -L https://github.com/databricks/tech-talks/raw/master/datasets/point_of_sale_simulated.zip
# MAGIC unzip -o point_of_sale_simulated.zip -d pos
# MAGIC 
# MAGIC mkdir -p /dbfs/tmp/realtime_pos/dest
# MAGIC mkdir -p /dbfs/tmp/realtime_pos/inventory_change
# MAGIC mkdir -p /dbfs/tmp/realtime_pos/inventory_snapshot
# MAGIC 
# MAGIC cp /tmp/pos/*.txt /dbfs/tmp/realtime_pos/dest
# MAGIC ls /dbfs/tmp/realtime_pos/dest

# COMMAND ----------


