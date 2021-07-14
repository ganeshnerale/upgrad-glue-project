# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 19:49:34 2021

@author: ganes
"""

import datetime
import os
import boto3
from airflow.operators.python_operator import PythonOperator
import sys
import time
from airflow import DAG


DEFAULT_ARGS = {
    "owner": "admin",
    "depends_on_past": False
}
#region_name=boto3.session.Session().region_name
glue_client=boto3.client('glue',region_name='us-east-1',aws_access_key_id='AKIA6RKZ7BBT33BLP', aws_secret_access_key='w5bjrc2+eFTsWFy2alt9VldBgLdhLR6XuUMM/B')

# Custom operator to trigger Glue job. This function utilizes boto3 client
def run_glue_job(**kwargs):
    job_name=kwargs['job_name']
    print("Starting Glue Job")
#Trigger the Databrew job and monitor it’s state.
    run_id=glue_client.start_job_run(JobName=job_name)
    state = glue_client.get_job_run(JobName=job_name,RunId=run_id['JobRunId'])
#Keep polling every 30 seconds to see if there is a status change in Job run.
    if state:
        status=state['JobRun']['JobRunState']
        while status not in ['SUCCEEDED']:
            print("Sleeping")
            time.sleep(10)
            job_status = glue_client.get_job_run(JobName=job_name,RunId=run_id['JobRunId'])
            status = job_status['JobRun']['JobRunState']
            print("Checking the status. Current status is",status)
            if status in ['STOPPED', 'FAILED', 'TIMEOUT']:
                sys.exit(1)

# Initialization of variable
#region_name=boto3.session.Session().region_name
today_date=str(datetime.date.today())


dag = DAG(
    dag_id="covid-dag",
    default_args=DEFAULT_ARGS,
    default_view="graph",
    ##schedule_interval="19 02 * * *", #### Change cron entry to schedule the job
    start_date=datetime.datetime(2021, 6, 5), ### Modify start date accordingly
    catchup=False, ### set it to True if backfill is required.
    tags=["example"],
)


state_table_extraction = PythonOperator(task_id='state_table_extraction',python_callable=run_glue_job,op_kwargs={'job_name':'state_table_extraction'},dag=dag)
state_dim_load_ready = PythonOperator(task_id='state_dim_load_ready',python_callable=run_glue_job,op_kwargs={'job_name':'state_dim_load_ready'},dag=dag)
state_dim_load = PythonOperator(task_id='state_dim_load',python_callable=run_glue_job,op_kwargs={'job_name':'state_dim_load'},dag=dag)

date_dim_load_ready = PythonOperator(task_id='date_dim_load_ready',python_callable=run_glue_job,op_kwargs={'job_name':'date_dim_load_ready'},dag=dag)
date_dim_load = PythonOperator(task_id='date_dim_load',python_callable=run_glue_job,op_kwargs={'job_name':'date_dim_load'},dag=dag)

covid_raw_extract_full = PythonOperator(task_id='covid_raw_extract_full',python_callable=run_glue_job,op_kwargs={'job_name':'covid_raw_extract_full'},dag=dag)
covid_fact_load_ready = PythonOperator(task_id='covid_fact_load_ready',python_callable=run_glue_job,op_kwargs={'job_name':'covid_fact_load_ready'},dag=dag)
covid_fact_load = PythonOperator(task_id='covid_fact_load',python_callable=run_glue_job,op_kwargs={'job_name':'covid_fact_load'},dag=dag)

state_table_extraction >>  state_dim_load_ready >> state_dim_load
date_dim_load_ready >> date_dim_load
date_dim_load >> covid_raw_extract_full >>covid_fact_load_ready >> covid_fact_load
state_dim_load >> covid_raw_extract_full >>covid_fact_load_ready >> covid_fact_load