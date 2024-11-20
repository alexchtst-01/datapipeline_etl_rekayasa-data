# scrapy_airflow_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import pandas as pd

def run_scrapy_spider():
    """Run the Scrapy spider to scrape data"""
    try:
        # Run the Scrapy spider using subprocess
        result = subprocess.run(
            ['scrapy', 'runspider', '/home/alex/personal/bisake2/scrap.py', '-o', '/home/alex/personal/bisake2/output.csv'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        print("Scrapy spider ran successfully!")
        return 'output.csv'

    except subprocess.CalledProcessError as e:
        raise Exception(f"Error running spider: {e.stderr.decode('utf-8')}")

def print_summary(file_path):
    """Print summary of scraped data using Pandas"""
    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)
        
        # Get total number of records
        total_records = len(df)
        
        # Print summary
        print(f"Total records scraped: {total_records}")
        print("First 5 records:")
        print(df.head())  # Display first 5 rows
        
    except Exception as e:
        print(f"Error loading the data or printing summary: {str(e)}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 11, 19),  # Adjust start date
}

dag = DAG(
    'coba_scrap_cwur',
    default_args=default_args,
    description='A DAG to scrape data and print a summary',
    schedule_interval=None,
    catchup=False,
)

# Task 1: Scrape data using Scrapy
scrape_data_task = PythonOperator(
    task_id='scrape_data',
    python_callable=run_scrapy_spider,
    dag=dag,
)

# Task 2: Print data summary
print_summary_task = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary,
    op_args=['output.csv'],  # Pass the output file to the function
    dag=dag,
)

# Set task order
scrape_data_task >> print_summary_task
