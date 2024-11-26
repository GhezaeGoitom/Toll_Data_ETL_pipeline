# time 
from datetime import timedelta
# The DAG object;
from airflow.models import DAG
# airflow Operators;
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago



#defining DAG arguments

default_args = {
    'owner': 'Gg',
    'start_date': days_ago(0),
    'email': ['gg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow ETL pipeline'
)



unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='mkdir -p /home/project/airflow/dags/finalassignment/tolldata && tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/tolldata',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d":" -f 1-4 /home/project/airflow/dags/finalassignment/tolldata/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)


extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d":" -f 1,5,7 /home/project/airflow/dags/finalassignment/tolldata/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
bash_command='cut -c55-57,59-64 /home/project/airflow/dags/finalassignment/tolldata/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
bash_command='paste /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,
)


transform_data = BashOperator(
    task_id='transform_data',
bash_command="""awk -F',' 'BEGIN{OFS=","} NR==1 {print $0} NR>1 {$4=toupper($4); print $0}' /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv""",
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv	>> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data