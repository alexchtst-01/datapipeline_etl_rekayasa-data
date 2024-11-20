# Tugas ETL (Extract Transform Load) data pipeline

## ETL Data Pipeline
Proyek ini bertujuan untuk membangun **ETL (Extract, Transform, Load)** data pipeline yang mengambil data dari sumber tertentu, membersihkan dan memproses data tersebut, kemudian menyimpannya ke dalam database yang terstruktur. Proyek ini dirancang untuk mempermudah analisis data dan menghasilkan insight yang dapat membantu pengambilan keputusan.

**Teknologi yang digunakan**:
- **Python** script base
- **Scrapy dan Selenium & Webdriver** untuk ekstraksi data
- **Airflow** scheduler data pipeline process
- **Pandas** data transformation
- **PostgreSQL** dan **Mongodb** database penyimpanan

## Anggota Kelompok

| **Nama**           | **NIM**        | 
|---------------------|-------------------|
| Alex Cinatra | 22/505820/TK/55377 | 
| Hafidh Husna | 22/498640/TK/54706 | 
| Ryan Krishandi Lukito | 22/49729/TK/54488 | 


## How to Run
Ikuti langkah-langkah berikut untuk menjalankan ETL data pipeline ini:

1. **clone Repository**
   ```bash
   git clone git@github.com:alexchtst-01/datapipeline_etl_rekayasa-data.git

   cd datapipeline_etl_rekayasa-data

2. **install environtment**
    ```bash
    python -m venv .venv
    
    source .venv/Script/activate
    
    pip install -r regirements.txt

3. **run web server**
    ```bash
    AIRFLOW_HOME=/home/path-to-your-dir/datapipeline_etl_rekayasa/airflow airflow webserver


4. **run web scheduler**
    ```bash
    AIRFLOW_HOME=/home/path-to-your-dir/datapipeline_etl_rekayasa/airflow airflow scheduler

**`disclaimer this used not working because of config airflow and .env`**

## Insight
[Streamlit ETL](https://dashboardinsightrekayasadata.streamlit.app)

## Model
- **Polynomial or Linear regression**
- **Kmeans Clustering**

## Notion Page
[ETL Data Pipeline Universitas Terbaik Dunia](https://scratch-monitor-859.notion.site/ETL-Data-Pipeline-Universitas-Terbaik-di-Dunia-1437796d5dfb80f69aeedc5cb55ede19)

## Video Presentasi
[Video Presentasi ETL](https://drive.google.com/file/d/17Y_GBb2bv5EkBG5WipxE68QaVB1Iq71a/view?usp=sharing)

