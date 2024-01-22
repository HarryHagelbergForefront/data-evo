
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

load_ext_stock_item_query = (
    f"CREATE OR REPLACE EXTERNAL TABLE data-evolution-harry.raw_wwi.ext_warehouse_stock_item ("
    f" StockItemID STRING, "
    f" StockItemName STRING, "
    f" SupplierID STRING, "
    f" ColorID STRING, "
    f" UnitPackageID STRING, "
    f" OuterPackageID STRING, "
    f" Brand STRING, "
    f" Size STRING, "
    f" LeadTimeDays STRING, "
    f" QuantityPerOuter STRING, "
    f" IsChillerStock STRING, "
    f" Barcode STRING, "
    f" TaxRate STRING, "
    f" UnitPrice STRING, "
    f" RecommendedRetailPrice STRING, "
    f" TypicalWeightPerUnit STRING, "
    f" MarketingComments STRING, "
    f" InternalComments STRING, "
    f" Photo STRING, "
    f" CustomFields STRING, "
    f" Tags STRING, "
    f" SearchDetails STRING, "
    f" LastEditedBy STRING, "
    f" ValidFrom STRING, "
    f" ValidTo STRING"
    f")"
    f" WITH PARTITION COLUMNS"
    f" (year STRING, month STRING)"
    f" OPTIONS("
    f" format = 'CSV', "
    f" uris = ['gs://raw-data-wwi/csv/warehouse.stockitem/year=2013/month=*'], "
    f" skip_leading_rows = 1, "
    f" hive_partition_uri_prefix = 'gs://raw-data-wwi/csv/warehouse.stockitem/', "
    f" require_hive_partition_filter = false"
    f");"
)

create_raw_stock_item_query = (
    f"CREATE OR REPLACE TABLE data-evolution-harry.raw_wwi.raw_warehouse_stock_item AS "
    f"SELECT * "
    f"FROM data-evolution-harry.raw_wwi.ext_warehouse_stock_item;"
)

insert_into_raw_stock_item_query = (
    f"INSERT INTO data-evolution-harry.raw_wwi.raw_warehouse_stock_item "
    f"SELECT * FROM data-evolution-harry.raw_wwi.ext_warehouse_stock_item;"
)

with DAG(
    'stock_item_load',
    default_args=default_args,
    schedule_interval=None
) as dag:
    load_stock_item_to_ext = BigQueryInsertJobOperator(
      task_id="load_ext_stock_item_job",
      configuration={
          "query": {
          "query": load_ext_stock_item_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location= "us-central1",
      project_id = "data-evolution-harry"
    )
    create_raw_stock_item_table = BigQueryInsertJobOperator(
      task_id="create_raw_stock_item_job",
      configuration={
          "query": {
          "query": create_raw_stock_item_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    insert_stock_item_to_raw = BigQueryInsertJobOperator(
      task_id="insert_into_raw_stock_item_job",
      configuration={
          "query": {
          "query": insert_into_raw_stock_item_query,
          "useLegacySql": False,
          "priority": "BATCH",
          }
      },
      location="us-central1",
      project_id="data-evolution-harry"
    )
    
load_stock_item_to_ext >> create_raw_stock_item_table >> insert_stock_item_to_raw