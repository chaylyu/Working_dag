FROM apache/airflow:2.5.3
RUN pip install --no-cache-dir pandas google-auth google-api-python-client
