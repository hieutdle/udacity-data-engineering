from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplays_table_create
)

create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artists_table_create
)

create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_create
)

create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.users_table_create
)

create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

all_tables_created = DummyOperator(task_id='All_tables_created', dag=dag)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    option="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    option="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    delete=True,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    delete=True,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    delete=True,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    delete=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_sql_list=[
        {
            'sql': 'SELECT COUNT(*) FROM public.artists;',
            'option': 'not equal',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM public.songplays WHERE songid IS NULL;',
            'option': 'equal',
            'val': 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG dependencies

# Step 1

start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table
start_operator >> create_songplays_table
start_operator >> create_artists_table
start_operator >> create_songs_table
start_operator >> create_users_table
start_operator >> create_time_table


# Step 2
create_staging_events_table >> all_tables_created
create_staging_songs_table >> all_tables_created
create_songplays_table >> all_tables_created
create_artists_table >> all_tables_created
create_songs_table >> all_tables_created
create_users_table >> all_tables_created
create_time_table >> all_tables_created

# Step 3
all_tables_created >> stage_events_to_redshift
all_tables_created >> stage_songs_to_redshift


# Step 4
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Step 5
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Step 6
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Step 7
run_quality_checks >> end_operator

