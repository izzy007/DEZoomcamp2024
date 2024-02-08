from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
import pyarrow as pa
from pyarrow import parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/dezoomcamp-2024-1f48036930c0.json"
bucket_name = 'mage-zoomcamp-week2-izzah'
project_id = 'dezoomcamp-2024'
table_name ="nyc_greentaxi_data"
root_path=f"{bucket_name}/{table_name}"

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    #print(df['lpep_pickup_datetime'].dt.date)
    df['lpep_pickup_date']=df['lpep_pickup_datetime'].dt.date
    pqtable=pa.Table.from_pandas(df)
    gcs=pa.fs.GcsFileSystem()
    
    pq.write_to_dataset(
        pqtable,
        filesystem=gcs,
        root_path=root_path,
        partition_cols=['lpep_pickup_date']

    )
