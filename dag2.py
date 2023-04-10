from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging 
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'GA',
    default_args=default_args,
    description='Retrieves Data from Google Analytics, processes it into a pandas dataframe, saves it into an S3 Bucket',
    schedule_interval='@daily'
)


def retrieve_data(**kwargs):
    start_date = datetime.utcnow() - timedelta(days=1)
    end_date = datetime.utcnow()
    print(1111)
    conn = BaseHook.get_connection('google_analytics_default')
    print(conn.extra_dejson)
    view_id = '188650309'
    credentials = service_account.Credentials.from_service_account_file(
        conn.extra_dejson['key_path'],  
        scopes=[u'https://www.googleapis.com/auth/analytics.readonly']
    )
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    dims = ['ga:totalEvents', 'ga:uniqueEvents', 'ga:eventValue']
    metrics = ['ga:dateHourMinute', 'ga:eventLabel', 'ga:eventAction', 'ga:dimension1', 'ga:date', 'ga:hour', 'ga:minute']

    response = analytics.reports().batchGet(
      body={
           'reportRequests': [
                {
        'viewId': view_id,
        'dateRanges': [{'startDate':  start_date.strftime('%Y-%m-%d'), 'endDate':  end_date.strftime('%Y-%m-%d')}],
        'metrics':[
            {"expression": "ga:totalEvents"},
            {"expression": "ga:uniqueEvents"},
            {"expression": "ga:eventValue"},
            ],
        'dimensions': [
            {"name": "ga:dateHourMinute"},
            {"name": "ga:eventLabel"},
            {"name": "ga:eventAction"},
            {"name": "ga:dimension1"},
            {"name": "ga:date"},
            {"name": "ga:hour"},
            {"name": "ga:minute"},
            ],
        }]
      }
).execute()
    print(response)
    logging.info('Request to GA API made successfully.')

     
    data_dic = {f"{i}": [] for i in dims + metrics}
    for report in response.get('reports', []):
        rows = report.get('data', {}).get('rows', [])
        for row in rows:
            for i, key in enumerate(dims):
                data_dic[key].append(row.get('dimensions', [])[i]) # Get dimensions
            dateRangeValues = row.get('metrics', [])
            for values in dateRangeValues:
                all_values = values.get('values', []) # Get metric values
                for i, key in enumerate(metrics):
                    if len(all_values) > i:
                        data_dic[key].append(all_values[i])
                    else:
                        data_dic[key].append(None)
    df = pd.DataFrame(data=data_dic)
    df.columns = [col.split(':')[-1] for col in df.columns]
    df.head()
    logging.info('Dataframe created successfully.')

    ti = kwargs['ti']
    ti.xcom_push(key='google_analytics_report', value=df.to_csv(index=False))

    logging.info('Data pushed successfully into xcom.')



def save_to_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    task_instance = kwargs['ti']
    current_time = datetime.utcnow()
    year = current_time.strftime('%Y')
    month = current_time.strftime('%m')
    file_name = f"{'source'}/{'report_name'}/{year}/{month}/{current_time.strftime('%d_%H%M%S')}.csv"
    data = task_instance.xcom_pull(key='google_analytics_report')
    print(data)
    s3_hook.load_string(
        string_data=data, 
        key=file_name, 
        bucket_name='mygoogleanalytics', 
        replace=True,
        encrypt=False
    )
    logging.info('Data saved to S3 successfully.')
    


retrieve_data_task = PythonOperator(
    task_id='retrieve_data_task',
    python_callable=retrieve_data,
    dag=dag
)

save_to_s3_task = PythonOperator(
    task_id='save_to_s3_task',
    python_callable=save_to_s3,
    dag=dag
)

retrieve_data_task >> save_to_s3_task

