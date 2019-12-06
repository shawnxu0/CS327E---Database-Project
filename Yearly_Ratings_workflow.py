import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    'start_date': datetime.datetime(2019, 11, 25)
}

staging_dataset = 'Yearly_Ratings_workflow_staging'
modeled_dataset = 'Yearly_Ratings_workflow_modeled'
bq_query_start = 'bq query --use_legacy_sql=false '


create_yearly_sql = 'create or replace table ' + modeled_dataset + '''.Yearly_Ratings as
                    select *
                    from ''' + staging_dataset + '''.Yearly_Ratings
                    '''

create_weekly_sql = 'create or replace table ' + modeled_dataset + '''.Weekly_Ratings as
                    select *
                    from ''' + staging_dataset + '''.Weekly_Ratings
                    '''

create_radio_sql = 'create or replace table ' + modeled_dataset + '''.Radio_Ratings as
                    select *
                    from ''' + staging_dataset + '''.Radio_Ratings
                    '''

with models.DAG(
        'Yearly_Ratings_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled_dataset = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)

    load_Yearly_Ratings = BashOperator(
            task_id='load_Yearly_Ratings',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Yearly_Ratings \
                         "gs://primary_set_imdb/yearly_songs/yearly_ratings.csv"',
            trigger_rule='one_success')
    
    load_Weekly_Ratings = BashOperator(
            task_id='load_Weekly_Ratings',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Weekly_Ratings \
                         "gs://primary_set_imdb/weekly_songs/weekly_ratings.csv"',
            trigger_rule='one_success')

    load_Radio_Ratings = BashOperator(
            task_id='load_Radio_Ratings',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Radio_Ratings \
                         "gs://primary_set_imdb/radio_rankings/radio_ratings.csv"',
            trigger_rule='one_success')

    split = DummyOperator(
            task_id='split',
            trigger_rule='all_done')
    
    create_Yearly_Ratings = BashOperator(
            task_id='create_Yearly_Ratings',
            bash_command=bq_query_start + "'" + create_yearly_sql + "'", 
            trigger_rule='one_success')
    
    create_Weekly_Ratings = BashOperator(
            task_id='create_Weekly_Ratings',
            bash_command=bq_query_start + "'" + create_weekly_sql + "'", 
            trigger_rule='one_success')
    
    create_Radio_Ratings = BashOperator(
            task_id='create_Radio_Ratings',
            bash_command=bq_query_start + "'" + create_radio_sql + "'", 
            trigger_rule='one_success')

    Yearly_Ratings_beam = BashOperator(
            task_id='Yearly_Ratings_beam',
            bash_command='python /home/jupyter/airflow/dags/transform_Yearly_Ratings_cluster_airflow.py')

    create_staging_dataset >> create_modeled_dataset >> split
    split >> load_Yearly_Ratings >> create_Yearly_Ratings >> Yearly_Ratings_beam
    split >> load_Weekly_Ratings >> create_Weekly_Ratings
    split >> load_Radio_Ratings >> create_Radio_Ratings