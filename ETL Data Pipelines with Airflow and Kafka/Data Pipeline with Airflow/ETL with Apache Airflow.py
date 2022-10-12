# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Uche Nnodim',
    'start_date': days_ago(0),
    'email': ['uchejudennodim@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the first task named unzip
unzip = BashOperator(
    task_id='unzip',
    bash_command='tar -xzvf tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)

# define the second task named extract
extract_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

extract_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/extract_tsv.csv',
    dag=dag,
)
extract_tsv1 = BashOperator(
    task_id='extract_tsv1',
    bash_command='cut -b 1 /home/project/airflow/dags/finalassignment/staging/extract_tsv.csv > /home/project/airflow/dags/finalassignment/staging/file1.csv',
    dag=dag,
)
extract_tsv2 = BashOperator(
    task_id='extract_tsv2',
    bash_command='cut -b 3-6 /home/project/airflow/dags/finalassignment/staging/extract_tsv.csv > /home/project/airflow/dags/finalassignment/staging/file2.csv',
    dag=dag,
)
extract_tsv3 = BashOperator(
    task_id='extract_tsv3',
    bash_command='cut -b 8-16 /home/project/airflow/dags/finalassignment/staging/extract_tsv.csv > /home/project/airflow/dags/finalassignment/staging/file3.csv',
    dag=dag,
)
consolidate_tsv = BashOperator(
    task_id='consolidate_tsv',
    bash_command='cd /home/project/airflow/dags/finalassignment/staging | paste -d"," file1.csv file2.csv file3.csv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)

extract_txt = BashOperator(
    task_id='extract_txt',
    bash_command='cut -b 59-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)

consolidate_csv = BashOperator(
    task_id='consolidate_csv',
    bash_command='cd /home/project/airflow/dags/finalassignment/staging | paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/consolidated_data.csv',
    dag=dag,
)

transform_csv = BashOperator(
    task_id='transform_csv',
    bash_command='tr [a-z] [A-Z] < /home/project/airflow/dags/finalassignment/staging/consolidated_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv,
    dag=dag,
)

unzip >> extract_csv >> extract_tsv >> extract_tsv1 >> extract_tsv2 >> extract_tsv3 >> consolidate_tsv >> extract_txt >> consolidate_csv >> transform_csv




# COPY THE DAG TO AIRFLOW
# Open terminal and run the following commands
# -  start_airflow
# -  cp finalassignment.py $AIRFLOW_HOME/dags
# -  airflow dags list
# -  airflow dags list|grep "my-first-dag"
# -  airflow tasks list my-first-dag