# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Xiaoqi Yu',
    'start_date': today,
    'email': ['xyu041262@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_tolldata',
    default_args=default_args,
    description='toll data ETL',
    schedule_interval=timedelta(days=1),
)

# define the task to download data
download = BashOperator(
    task_id='download',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag=dag,
)


# define the task to unzip data
unzip = BashOperator(
    task_id='unzip_data',
    bash_command='tar zvxf tolldata.tgz',
    dag=dag,
)


# define the task to extract data from csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1,2,3,4 -d"#" vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# define the task to extract data from tsv

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 -d"\t" tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.tsv',
    dag=dag,
)

# define the task to extract data from fixed width file

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk "{print substr($0, 59, 3), substr($0, 63, 5)}" payment-data.txt > /home/project/airflow/dags/fixed_width_data.tsv',
    dag=dag,
)

# define the task to consolidate data
consolidate_data= BashOperator(
    task_id='consolidate data',
    bash_command='cd /home/project/airflow/dags/'
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# define the task to transform data
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]"< extracted_data.csv > transformed_data.csv',
    dag=dag,
)

#task pipeline

download >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


