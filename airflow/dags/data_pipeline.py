from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from etl_functions import WebScraper, DataTransformer, LoadRawData, MergeData
import pandas as pd
import pymongo
import psycopg2
from dotenv import load_dotenv, dotenv_values

load_dotenv()

def extract_th(**kwargs):
    scraper = WebScraper()
    try:
        th_data = scraper.extract_th_data()
        logging.info("Scraped Times Higher Education data.")
        kwargs['ti'].xcom_push(key='th_data', value=th_data)
    finally:
        scraper.close_driver()

def extract_ntu(**kwargs):
    scraper = WebScraper()
    try:
        ntu_data = scraper.extract_ntu_data()
        logging.info("Scraped NTU data.")
        kwargs['ti'].xcom_push(key='ntu_data', value=ntu_data)
    finally:
        scraper.close_driver()

def transform_th(**kwargs):
    ti = kwargs['ti']
    th_data = ti.xcom_pull(task_ids='scrape_th_data', key='th_data')
    
    if th_data is not None:
        df = pd.DataFrame(th_data)
        transformer = DataTransformer()
        th_data_transformed = transformer.transform_th(df)
        logging.info("Times Higher Education data transformed.")
        kwargs['ti'].xcom_push(key='th_data_transformed', value=th_data_transformed.to_dict())
        logging.info("NTU data send.")
        return th_data_transformed.to_dict()
    else:
        logging.error("No data found in XCom for TH!")
        return {}

def transform_ntu(**kwargs):
    ti = kwargs['ti']
    ntu_data = ti.xcom_pull(task_ids='scrape_ntu_data', key='ntu_data')
    
    if ntu_data is not None:
        df = pd.DataFrame(ntu_data)
        transformer = DataTransformer()
        ntu_data_transformed = transformer.transform_ntu(df)
        logging.info("NTU data transformed.")
        kwargs['ti'].xcom_push(key='ntu_data_transformed', value=ntu_data_transformed.to_dict())
        logging.info("NTU data send.")
        return ntu_data_transformed.to_dict()
    else:
        logging.error("No data found in XCom for NTU!")
        return {}

def load_raw_data(**kwargs):
    ti = kwargs['ti']
    
    load_success = ti.xcom_pull(task_ids='load_raw_data', key='load_raw_data_success')
    
    if load_success:
        raise AirflowSkipException("load_raw_data already succeeded in a previous attempt.")
    
    client = pymongo.MongoClient(dotenv_values('.env')['MONGO_CONN'])
    collection = client['raw_data']['data']

    # Pull scraped data from XCom
    ntu_data = ti.xcom_pull(task_ids='scrape_ntu_data', key='ntu_data')
    th_data = ti.xcom_pull(task_ids='scrape_th_data', key='th_data')

    try:
        if ntu_data:
            collection.insert_one(ntu_data)
            logging.info('Data NTU successfully inserted.')
        
        if th_data:
            collection.insert_one(th_data)
            logging.info('Data TH successfully inserted.')
    except Exception as e:
        logging.error(f"Failed to insert data into MongoDB: {str(e)}")
        raise
    
    # Mark the task as successful in XCom
    ti.xcom_push(key='load_raw_data_success', value=True)

def merge_transformed_data(**kwargs):
    # Pull the transformed data from XCom
    ntu_data = kwargs['ti'].xcom_pull(task_ids='transform_ntu_data', key='ntu_data_transformed')
    th_data = kwargs['ti'].xcom_pull(task_ids='transform_th_data', key='th_data_transformed')

    if ntu_data and th_data:  # Ensure both data sets are not empty
        data_merger = MergeData(ntu_data=ntu_data, th_data=th_data)
        data_merged = data_merger.merge()
        kwargs['ti'].xcom_push(key='data_merge', value=data_merged)  # Store merged data in XCom
        logging.info("Data successfully merged.")
        logging.info(data_merged)
    else:
        logging.error("Missing transformed data for merging.")

def load_final_data(**kwargs):
    load_dotenv()
    aiven_url = dotenv_values('.env')['AIVEN_CONN']
    conn = psycopg2.connect(aiven_url)

    df_combined = kwargs['ti'].xcom_pull(task_ids='merge_data', key='data_merge')

    df_combined = pd.DataFrame(df_combined)

    logging.info(df_combined)

    create_table_query = '''
        CREATE TABLE IF NOT EXISTS university_rankings (
            rank INT,
            university TEXT,
            location TEXT,
            overall FLOAT,
            teaching FLOAT,
            research_environment FLOAT,
            research_quality FLOAT,
            industry FLOAT,
            international_outlook FLOAT,
            eleven_years_articles FLOAT,
            eleven_years_citations FLOAT,
            current_years_citations FLOAT,
            average_citations FLOAT,
            h_index FLOAT,
            hi_ci_papers FLOAT,
            extracted_year INT
        );
    '''
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully (if not already existing).")

    for _, row in df_combined.iterrows():
        insert_query = '''
        INSERT INTO public.university_rankings(
            rank, university, location, 
            overall, teaching, research_environment, research_quality,
            industry, international_outlook, eleven_years_articles, 
            eleven_years_citations, current_years_citations, average_citations, 
            h_index, hi_ci_papers, extracted_year)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cur.execute(
            insert_query, (row['Rank'], row['University'], row['Location'], 
                          row['Overall'], row['Teaching'], row['Research Environment'], 
                          row['Research Quality'], row['Industry'], row['International Outlook'], 
                          row['11 Years Articles'], row['11 Years Citations'], row['Current Years Citations'], 
                          row['Average Citations'], row['H-Index'], row['Hi-Ci Papers'], row['Year Scrap']
            ))
    conn.commit()
    logging.info("Data inserted successfully!")

    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'catchup': False,
}

# Define the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Times Higher Education and NTU data pipeline',
    schedule_interval=None,
    catchup=False,
)

# Task to extract TH data
scrape_th_data_task = PythonOperator(
    task_id='scrape_th_data',
    python_callable=extract_th,
    provide_context=True,
    dag=dag,
)

# Task to transform TH data
transform_th_task = PythonOperator(
    task_id='transform_th_data',
    python_callable=transform_th,
    provide_context=True,
    dag=dag,
)

# Task to extract NTU data
scrape_ntu_data_task = PythonOperator(
    task_id='scrape_ntu_data',
    python_callable=extract_ntu,
    provide_context=True,
    dag=dag,
)

# Task to transform NTU data
transform_ntu_task = PythonOperator(
    task_id='transform_ntu_data',
    python_callable=transform_ntu,
    provide_context=True,
    dag=dag,
)

# Task to load data into mongodb
task_load_raw_data_task = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    provide_context=True,
    dag=dag,
)

task_merge_data = PythonOperator(
    task_id='merge_data',
    python_callable=merge_transformed_data,
    provide_context=True,
    dag=dag,
)

load_final_data_task = PythonOperator(
    task_id='load_final_data',
    python_callable=load_final_data,
    provide_context=True,
    dag=dag,
)

# Define task
scrape_th_data_task >> task_load_raw_data_task
scrape_ntu_data_task >> task_load_raw_data_task

scrape_th_data_task >> transform_th_task
scrape_ntu_data_task >> transform_ntu_task

transform_th_task >> task_merge_data
transform_ntu_task >> task_merge_data

task_merge_data >> load_final_data_task