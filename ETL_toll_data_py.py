# Import the libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
import csv
import pandas as pd


url = ' https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination = '/home/project/airflow/dags/python_etl/staging'
extract_path = '/home/project/airflow/dags/python_etl/staging/toll_data'
csv_extracted_file = '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
tsv_extracted_file = '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
txt_extracted_file = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
extracted_data = '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
transformed_data = '/home/project/airflow/dags/python_etl/staging/transformed_data.csv'


#defining DAG arguments
default_args = {
    'owner': 'Gg',
    'start_date': days_ago(0),
    'email': ['gg@gmail.com'],
    'email_on_failure': True,
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



def download_dataset(url, destination):
    # Send a GET request to the URL
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check if the request was successful

    # Open the destination file in write-binary mode
    with open(destination, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"File downloaded and saved to {destination}")



def untar_dataset():
    global destination
    global extract_path
    # Ensuring the extract_path exists
    os.makedirs(extract_path, exist_ok=True)
    
    # Open the tar file and extract
    with tarfile.open(destination, 'r:*') as tar:
        tar.extractall(path=extract_path)
    print(f"Dataset extracted to {extract_path}")


def extract_data_from_csv():
    global extract_path
    global csv_extracted_file

    # column indices to extract (0-based index)
    columns_to_extract = [0, 1, 2, 3]  

    # headers for the output file
    headers = ["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"]
    
    # Open the input CSV for reading and output CSV for writing
    with open('f{extract_path}/vehicle-data.csv', mode='r') as infile, open(csv_extracted_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write the headers to the output file
        writer.writerow(headers)
        
        # Write only the selected columns to the output file
        for row in reader:
            selected_columns = [row[i] for i in columns_to_extract]
            writer.writerow(selected_columns)
    
    print(f"Data extracted and saved as {csv_extracted_file}")



def extract_data_from_tsv():
    global extract_path
    global tsv_extracted_file
    # column indices to extract (0-based index for columns 1, 3, 4, and 5)
    columns_to_extract = [0, 4, 6]  

    # headers for the output file
    headers = ["Number of axles", "Tollplaza id", "Tollplaza code"]

    # Open the input TSV for reading and output CSV for writing
    with open('f{extract_path}/tollplaza-data.tsv', mode='r') as infile, open(tsv_extracted_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)

        # Write headers to the output file
        writer.writerow(headers)

        # Write only the selected columns to the output file
        for row in reader:
            # Select the specified columns
            selected_columns = [row[i] for i in columns_to_extract]
            writer.writerow(selected_columns)
    
    print(f"Data extracted and saved as {tsv_extracted_file}")



def extract_data_from_fixed_width():
    global extract_path
    global txt_extracted_file
    # column names and their corresponding fixed-width positions
    column_names = ["Type of Payment code", "Vehicle Code"]
    column_widths = [3, 6]  

    # start and end positions (for example purposes; adjust based on file structure)
    colspecs = [(55,57),(59,64)]  # Adjust based on actual fixed-width positions

    # Read the fixed-width file
    df = pd.read_fwf(f'{extract_path}/payment-data.txt', colspecs=colspecs, names=column_names)

    # Save the extracted data to a CSV file
    df.to_csv(txt_extracted_file, index=False)
    print(f"Data extracted and saved as {txt_extracted_file}")

def consolidate_data(csv_file, tsv_file, fixed_width_file, output_csv):
    global csv_extracted_file, tsv_extracted_file, txt_extracted_file, extracted_data

    # Read each file into a DataFrame
    df_csv = pd.read_csv(csv_extracted_file)
    df_tsv = pd.read_csv(tsv_extracted_file)
    df_fixed_width = pd.read_csv(fixed_width_file)

    # Concatenate the DataFrames
    consolidated_df = pd.concat([df_csv, df_tsv, df_fixed_width], ignore_index=True)

    # Save the consolidated data to a new CSV file
    consolidated_df.to_csv(extracted_data, index=False)
    print(f"Data consolidated and saved in {extracted_data}")

def transform_data(input_csv, output_csv):
    global extracted_data, transformed_data
    # Read the input CSV file
    df = pd.read_csv(extracted_data)

    # Check if 'vehicle_type' column exists, then transform it
    if 'vehicle_type' in df.columns:
        df['vehicle_type'] = df['vehicle_type'].str.upper()
    else:
        print("Column 'vehicle_type' does not exist in the input file.")
        return

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(transformed_data), exist_ok=True)

    # Save the transformed data to the output CSV file
    df.to_csv(transformed_data, index=False)
    print(f"Transformed data saved as {transformed_data}")



download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_dataset,
    dag=dag,
)

unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)


extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)


extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)


extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


download_data_task >> unzip_data_task >> [extract_data_from_csv, extract_data_from_tsv_task, extract_data_from_fixed_width_task] >> consolidate_data_task >> transform_data_task 
