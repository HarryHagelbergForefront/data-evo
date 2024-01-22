
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

load_ext_customer_query = (
  f"CREATE OR REPLACE EXTERNAL TABLE `data-evolution-harry.raw_wwi.ext_sales_customer`("
  f" CustomerID STRING, "
  f" CustomerName STRING, "
  f" BillToCustomerID STRING, "
  f" CustomerCategoryID STRING, "
  f" BuyingGroupID STRING, "
  f" PrimaryContactPersonID STRING, "
  f" AlternateContactPersonID STRING, "
  f" DeliveryMethodID STRING, "
  f" DeliveryCityID STRING, "
  f" PostalCityID STRING, "
  f" CreditLimit STRING, "
  f" AccountOpenedDate STRING, "
  f" StandardDiscountPercentage STRING, "
  f" IsStatementSent STRING, "
  f" IsOnCreditHold STRING, "
  f" PaymentDays STRING, "
  f" PhoneNumber STRING, "
  f" FaxNumber STRING, "
  f" DeliveryRun STRING, "
  f" RunPosition STRING, "
  f" WebsiteURL STRING, "
  f" DeliveryAddressLine1 STRING, "
  f" DeliveryAddressLine2 STRING, "
  f" DeliveryPostalCode STRING, "
  f" DeliveryLocation STRING, "
  f" PostalAddressLine1 STRING, "
  f" PostalAddressLine2 STRING, "
  f" PostalPostalCode STRING, "
  f" LastEditedBy STRING, "
  f" ValidFrom STRING, "
  f" ValidTo STRING"
  f")"
  f"WITH PARTITION COLUMNS("
  f" year STRING, "
  f" month STRING"
  f")"
  f"OPTIONS ("
  f" format = 'CSV', "
  f" uris = ['gs://raw-data-wwi/csv/sales.customers/year=2013/month=*'], "
  f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/sales.customers', "
  f" skip_leading_rows = 1"
  f");"
)

create_raw_customer_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_sales_customer AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_sales_customer;"
)

insert_into_raw_customer_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_sales_customer "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_sales_customer;"
)

with DAG(
    'customer_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_customer_to_ext = BigQueryInsertJobOperator(
      task_id="load_ext_customer_job",
      configuration={
          "query": {
          "query": load_ext_customer_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry"
    )
    create_raw_customer_table = BigQueryInsertJobOperator(
      task_id="create_raw_customer_job",
      configuration={
          "query": {
          "query": create_raw_customer_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    insert_customer_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_customer_job",
      configuration={
          "query": {
          "query": insert_into_raw_customer_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )

load_customer_to_ext >> create_raw_customer_table >> insert_customer_to_raw