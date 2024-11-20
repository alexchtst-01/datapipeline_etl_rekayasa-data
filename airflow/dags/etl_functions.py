from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pymongo
import logging
import datetime

class WebScraper:
    def __init__(self):
        """Initializes WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        executable_path = ChromeDriverManager().install()
        self.driver = webdriver.Chrome(service=webdriver.chrome.service.Service(executable_path), options=chrome_options)


    def close_driver(self):
        """Close the WebDriver instance."""
        self.driver.quit()

    def extract_th_data(self):
        """Extract data from Times Higher Education rankings."""
        url = "https://www.timeshighereducation.com/world-university-rankings/latest/world-ranking#!/length/-1/sort_by/rank/sort_order/asc/cols/scores"
        self.driver.get(url)

        ranks, names, overall_scores = [], [], []
        teaching_scores, research_env_scores = [], []
        research_quality_scores, industry_scores = [], []
        intl_outlook_scores = []

        def extract_data():
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) > 6:
                    ranks.append(cells[0].text)
                    names.append(cells[1].text)
                    overall_scores.append(cells[2].text)
                    teaching_scores.append(cells[3].text)
                    research_env_scores.append(cells[4].text)
                    research_quality_scores.append(cells[5].text)
                    industry_scores.append(cells[6].text)
                    intl_outlook_scores.append(cells[7].text)

        while True:
            extract_data()
            try:
                next_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a.next"))
                )
                next_button.click()
            except Exception:
                break

        df = pd.DataFrame({
            "Rank": ranks,
            "Name": names,
            "Overall": overall_scores,
            "Teaching": teaching_scores,
            "Research Environment": research_env_scores,
            "Research Quality": research_quality_scores,
            "Industry": industry_scores,
            "International Outlook": intl_outlook_scores
        })
        return df.to_dict()

    def extract_ntu_data(self):
        """Extract data from NTU rankings."""
        url = "https://nturanking.csti.tw/ranking/OverallRanking/"
        self.driver.get(url)

        select_element = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "Top800University_length"))
        )
        select = Select(select_element)
        select.select_by_value("-1")  # Select "All"

        ranks, names, score, eleven_y_articles = [], [], [], []
        eleven_y_citations, current_y_citations = [], [],
        avg_citations, h_index, hici_papers = [], [], []
        hi_imp_journal = []

        def extract_data():
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) > 6:
                    ranks.append(cells[0].text)
                    names.append(cells[1].text)
                    score.append(cells[3].text)
                    eleven_y_articles.append(cells[4].text)
                    eleven_y_citations.append(cells[5].text)
                    current_y_citations.append(cells[6].text)
                    avg_citations.append(cells[7].text)
                    h_index.append(cells[8].text)
                    hici_papers.append(cells[9].text)
                    hi_imp_journal.append(cells[10].text)

        extract_data()
        df = pd.DataFrame({
            "Rank": ranks,
            "Name": names,
            "Score": score,
            "11 Years Articles": eleven_y_articles,
            "11 Years Citations": eleven_y_citations,
            "Current Years Citations": current_y_citations,
            "Average Citations": avg_citations,
            "H-Index": h_index,
            "Hi-Ci Papers": hici_papers,
            "Hi Impact Journal": hi_imp_journal
        })
        return df.to_dict()

class DataTransformer:
    def __init__(self):
        pass
    
    def transform_th(self, df):
        """Transform Times Higher Education DataFrame."""
        df = df[df['Rank'] != "Reporter"]
        df = df[df['Rank'] != "1501+"]
        
        df['Name'] = df['Name'].str.replace(r'\n', '-', regex=True)
        # Identify rows with and without the delimiter '-'
        has_delimiter = df['Name'].str.contains('-')
        no_delimiter = df[~has_delimiter]

        # If there are rows without the delimiter, handle them (e.g., log or fill with NaN)
        if not no_delimiter.empty:
            print("Rows without delimiter:")
            print(no_delimiter)

        # Split the 'Name' column into 'University' and 'Location', ensuring NaN for missing parts
        split_cols = df['Name'].str.split('-', n=1, expand=True)
        df['University'] = split_cols[0]
        df['Location'] = split_cols[1]

        # Drop the 'Name' column
        df = df.drop(columns='Name')

        last_col = df.columns[-1]  # Kolom terakhir
        df.insert(1, last_col, df.pop(last_col))

        last_col = df.columns[-1]  # Kolom terakhir
        df.insert(1, last_col, df.pop(last_col))

        return df

    def transform_ntu(self, df):
        """Transform NTU DataFrame."""
        df = pd.DataFrame(df)
        df = df[:594]
        df_insights = df.iloc[:, 3:9].apply(pd.to_numeric, errors='coerce')
        df = pd.concat([df.iloc[:594, :2], df_insights], axis=1)
        return df

class LoadRawData:
    def __init__(self):
        self.__uri__ = "mongodb+srv://alexcinatrahutasoit:x2LD4i3e3Tmqxqht@data-sources.vqzrk.mongodb.net/?retryWrites=true&w=majority&appName=data-sources"
        self.client = pymongo.MongoClient(self.__uri__)
        self.RawCollection = self.client['raw_data']['data']

    def insert(self, data):
        '''data dalam bentuk array dictionary'''
        try:
            '''
            karena datanya dalam bentuk dataframe.to_dict
            nanti kita transform dulu ke dalam bentuk 
            to_dict ke to_dict(orient='records')
            '''
            reformatted_data = [
                {key: data[key][i] for key in data.keys()} for i in range(len(next(iter(data.values()))))
            ]
            logging.info(f"Inserting {len(reformatted_data)} records into MongoDB.")
            self.RawCollection.insert_many(reformatted_data)
            return True
        except Exception as e:
            print(e)
            return False

class MergeData:
    def __init__(self, ntu_data, th_data):
        self.data_ntu = pd.DataFrame(ntu_data)
        self.data_th = pd.DataFrame(th_data)
    
    def merge(self):
        # Ensure ntu and th are set correctly
        ntu = self.data_ntu
        th = self.data_th
        
        if 'Unnamed: 0' in ntu.columns:
            ntu = ntu.drop(columns='Unnamed: 0')

        if 'Unnamed: 0' in th.columns:
            th = th.drop(columns='Unnamed: 0')

        ntu = ntu.rename(columns={"Name": "University"})
        
        # Perform the merge
        df_combined = pd.merge(th, ntu, on='University', how='inner')

        # Drop the 9th column (index 9)
        df_combined = df_combined.drop(df_combined.columns[9], axis=1)

        # Calculate the 'Overall' score
        numeric_columns = ['Teaching', 'Research Environment', 'Research Quality', 'Industry', 'International Outlook']
        df_combined[numeric_columns] = df_combined[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # Handle NaN values (fill with 0, or drop rows)
        df_combined[numeric_columns] = df_combined[numeric_columns].fillna(0)  # or use dropna()

        # Calculate the 'Overall' score
        df_combined['Overall'] = df_combined[numeric_columns].mean(axis=1)

        # Sort by 'Overall' score
        df_combined.sort_values(by='Overall', ascending=False, inplace=True)

        # Assign rankings
        df_combined['Rank_x'] = range(1, len(df_combined) + 1)
        df_combined.rename(columns={"Rank_x": "Rank"}, inplace=True)

        # Add the current year
        current_year = datetime.datetime.now().year
        df_combined['Year Scrap'] = current_year

        return df_combined.to_dict()
