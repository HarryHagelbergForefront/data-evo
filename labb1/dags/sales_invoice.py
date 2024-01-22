
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

load_ext_invoice_query = (
    f"CREATE OR REPLACE EXTERNAL TABLE data-evolution-harry.raw_wwi.ext_sales_invoice ("
    f" InvoiceID STRING, "
    f" CustomerID STRING, "
    f" BillToCustomerID STRING, "
    f" OrderID STRING, "
    f" DeliveryMethodID STRING, "
    f" ContactPersonID STRING, "
    f" AccountsPersonID STRING, "
    f" SalespersonPersonID STRING, "
    f" PackedByPersonID STRING, "
    f" InvoiceDate STRING, "
    f" CustomerPurchaseOrderNumber STRING, "
    f" IsCreditNote STRING, "
    f" CreditNoteReason STRING, "
    f" Comments STRING, "
    f" DeliveryInstructions STRING, "
    f" InternalComments STRING, "
    f" TotalDryItems STRING, "
    f" TotalChillerItems STRING, "
    f" DeliveryRun STRING, "
    f" RunPosition STRING, "
    f" ReturnedDeliveryData STRING, "
    f" ConfirmedDeliveryTime STRING, "
    f" ConfirmedReceivedBy STRING, "
    f" LastEditedBy STRING, "
    f" LastEditedWhen STRING"
    f")"
    f" WITH PARTITION COLUMNS"
    f" (year STRING, month STRING)"
    f" OPTIONS("
    f" format = 'CSV', "
    f" uris = ['gs://raw-data-wwi/csv/sales.invoices/year=2013/month=*', 'gs://raw-data-wwi/csv/sales.invoices/year=2016/month=*'], "
    f" skip_leading_rows = 1, "
    f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/sales.invoices/', "
    f" require_hive_partition_filter = false"
    f");"
)

create_raw_invoice_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_sales_invoice AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_sales_invoice;"
)

insert_into_raw_invoice_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_sales_invoice "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_sales_invoice;"
)

with DAG(
    'invoice_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_invoice_to_ext = BigQueryInsertJobOperator(
      task_id="load_ext_invoice_job",
      configuration={
          "query": {
          "query": load_ext_invoice_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry"
    )
    create_raw_invoice_table = BigQueryInsertJobOperator(
      task_id="create_raw_invoice_job",
      configuration={
          "query": {
          "query": create_raw_invoice_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    insert_invoice_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_invoice_job",
      configuration={
          "query": {
          "query": insert_into_raw_invoice_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    
load_invoice_to_ext >> create_raw_invoice_table >> insert_invoice_to_raw