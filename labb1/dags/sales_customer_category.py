
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

load_ext_customer_category_query = (
    f"CREATE OR REPLACE EXTERNAL TABLE data-evolution-harry.raw_wwi.ext_sales_customer_category ("
    f" CustomerCategoryID STRING, "
    f" CustomerCategoryName STRING, "
    f" LastEditedBy STRING, "
    f" ValidFrom STRING, "
    f" ValidTo STRING"
    f")"
    f" WITH PARTITION COLUMNS"
    f" (year STRING, month STRING)"
    f" OPTIONS("
    f" format = 'CSV', "
    f" uris = ['gs://raw-data-wwi/csv/sales.customercategories/year=2013/month=*'], "
    f" skip_leading_rows = 1, "
    f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/sales.customercategories/', "
    f" require_hive_partition_filter = false"
    f");"
)

create_raw_customer_category_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_sales_customer_category AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_sales_customer_category;"
)

insert_into_raw_customer_category_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_sales_customer_category "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_sales_customer_category;"
)

with DAG(
    'customer_category_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_customer_category_to_ext = BigQueryInsertJobOperator(
      task_id="create_ext_customer_category_job",
      configuration={
          "query": {
          "query": load_ext_customer_category_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry",
    )
    create_raw_customer_category_table = BigQueryInsertJobOperator(
      task_id="create_raw_customer_category_job",
      configuration={
          "query": {
          "query": create_raw_customer_category_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    insert_customer_category_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_customer_category_job",
      configuration={
          "query": {
          "query": insert_into_raw_customer_category_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )

load_customer_category_to_ext >> create_raw_customer_category_table >> insert_customer_category_to_raw