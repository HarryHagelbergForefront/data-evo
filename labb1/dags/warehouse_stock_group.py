
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
  BigQueryInsertJobOperator,
)
from datetime import datetime
default_args = {
  'owner': 'airflow',
  'start_date': datetime(2024, 1, 22),
  'retries': 1,
}

load_ext_stock_group_query = (
    f"CREATE OR REPLACE EXTERNAL TABLE data-evolution-harry.raw_wwi.ext_warehouse_stock_group ("
    f" StockGroupID STRING, "
    f" StockGroupName STRING, "
    f" LastEditedBy STRING"
    f")"
    f" WITH PARTITION COLUMNS"
    f" (year STRING, month STRING)"
    f" OPTIONS("
    f" format = 'CSV', "
    f" uris = ['gs://raw-data-wwi/csv/warehouse.stockgroups/year=2013/month=*'], "
    f" skip_leading_rows = 1, "
    f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/warehouse.stockgroups/', "
    f" require_hive_partition_filter = false"
    f");"
)

create_raw_stock_group_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_warehouse_stock_group AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_warehouse_stock_group;"
)

insert_into_raw_stock_group_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_warehouse_stock_group "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_warehouse_stock_group;"
)

with DAG(
    'stock_group_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_stock_group_to_ext = BigQueryInsertJobOperator(
      task_id="load_ext_stock_group_job",
      configuration={
          "query": {
          "query": load_ext_stock_group_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry"
    )
    create_raw_stock_group_table = BigQueryInsertJobOperator(
      task_id="create_raw_stock_group_job",
      configuration={
          "query": {
          "query": create_raw_stock_group_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    insert_stock_group_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_stock_group_job",
      configuration={
          "query": {
          "query": insert_into_raw_stock_group_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )

load_stock_group_to_ext >> create_raw_stock_group_table >> insert_stock_group_to_raw