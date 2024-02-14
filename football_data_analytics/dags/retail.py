from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
@dag(
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src=r'C:\Users\Dell\OneDrive\Bureaublad\football_data_analytics\football_data_analytics\include\data\stadium_cleaned_2024-02-07_13_42_47.545107.csv',
        dst='raw/online_retail.csv',
        bucket='liptakattila_online_retail',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='dataengineers-407810.football_project_dataset',
        gcp_conn_id='gcp'

    )


retail()