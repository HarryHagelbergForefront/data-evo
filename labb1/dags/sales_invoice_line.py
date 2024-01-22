
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

load_ext_invoice_line_query = (
    f"CREATE OR REPLACE EXTERNAL TABLE data-evolution-harry.raw_wwi.ext_sales_invoice_line ("
    f" InvoiceLineID STRING, "
    f" InvoiceID STRING, "
    f" StockItemID STRING, "
    f" Description STRING, "
    f" PackageTypeID STRING, "
    f" Quantity STRING, "
    f" UnitPrice STRING, "
    f" TaxRate STRING, "
    f" TaxAmount STRING, "
    f" LineProfit STRING, "
    f" ExtendedPrice STRING, "
    f" LastEditedBy STRING, "
    f" LastEditedWhen STRING"
    f")"
    f" WITH PARTITION COLUMNS"
    f" (year STRING, month STRING)"
    f" OPTIONS("
    f" format = 'CSV', "
    f" uris = ['gs://raw-data-wwi/csv/sales.invoicelines/year=2016/month=*', 'gs://raw-data-wwi/csv/sales.invoicelines/year=2013/month=*'], "
    f" skip_leading_rows = 1, "
    f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/sales.invoicelines/', "
    f" require_hive_partition_filter = false"
    f");"
)

create_raw_invoice_line_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_sales_invoice_line AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_sales_invoice_line;"
)

insert_into_raw_invoice_line_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_sales_invoice_line "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_sales_invoice_line;"
)

with DAG(
    'invoice_line_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_invoice_line = BigQueryInsertJobOperator(
      task_id="load_ext_invoice_line_job",
      configuration={
          "query": {
          "query": load_ext_invoice_line_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry",
    )
    create_raw_invoice_line_table = BigQueryInsertJobOperator(
      task_id="create_raw_invoice_line_job",
      configuration={
          "query": {
          "query": create_raw_invoice_line_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"  
    )
    insert_invoice_line_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_invoice_line_job",
      configuration={
          "query": {
          "query": insert_into_raw_invoice_line_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )

load_invoice_line >> create_raw_invoice_line_table >> insert_invoice_line_to_raw