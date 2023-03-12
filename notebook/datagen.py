# Databricks notebook source
from datetime import datetime, timedelta
from time import sleep

class DataGen(object):
    def __init__(self, spark_session, tablename, dt_colname, start_datetime, export_dir='/dbfs/tmp', max_ite=100, dt_step=timedelta(days=1), sleep_time=1, dump_format='csv'):
        self.dt_step = dt_step
        self.speep_time = sleep_time # seconds
        self.start_datetime = start_datetime #datetime.fromisoformat('2021-01-01')

        self.export_dir = export_dir
        self.spark_session = spark_session
        self.table_name = tablename
        self.dt_colname = dt_colname
        self.max_ite = max_ite
        self.dump_format = dump_format

    def start(self):
        dt_now = self.start_datetime
        for i in range(self.max_ite):
            print(f'iteration: {i}')
            dt_in  = dt_now
            dt_out = dt_in + self.dt_step
            print(f'dt_now => {dt_now.isoformat()}')

            sql = f'''
                select * from {self.table_name}
                where {self.dt_colname} >= '{dt_in.isoformat()}'
                    and   {self.dt_colname} < '{dt_out.isoformat()}'
                order by {self.dt_colname}
            '''
            pdf = self.spark_session.sql(sql).toPandas()
            if len(pdf) > 0:
                if self.dump_format == 'csv':
                    pdf.to_csv(f'{self.export_dir}/{self.table_name}_{int(dt_in.timestamp())}.csv', index=False)
                elif self.dump_format == 'json':
                    pdf.to_json(f'{self.export_dir}/{self.table_name}_{int(dt_in.timestamp())}.json', orient='records', lines=True, date_unit='s')
                else:
                    raise Exception(f'dump_format error: format must be csv or json - your dump_format => {self.dump_format}')

            # for next iteration
            dt_now += self.dt_step
            sleep(self.speep_time)

    

# COMMAND ----------


