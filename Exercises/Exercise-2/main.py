import requests
import pandas as pd
from bs4 import BeautifulSoup
import dask.dataframe as dd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def get_download_urls(base_url):
    download_links = []
    page = requests.get(base_url)
    soup = BeautifulSoup(page.content, "html.parser")
    table = soup.find('table')
    for row in table.find_all('tr'):
        data = [x.text.strip() for x in row.find_all('td')]
        if '2024-01-19 10:27' in data:
            file_name = data[0]
            download_link = base_url + file_name
            download_links.append(download_link)
    return download_links
            
def process_csv(url):
    df = pd.read_csv(url)
    return df


def dask_alternative(urls_list):
    frames = [process_csv(url) for url in urls_list]
    result = pd.concat(frames)
    df_sorted = result.sort_values('HourlyDryBulbTemperature')
    print(df_sorted.head())

def main():
    # your code here
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    urls_list = get_download_urls(base_url=base_url)
    # since it returns more than one file grabbing only the first one from list for exercise
    first_url = urls_list[0]
    df = pd.read_csv(first_url)
    df_sorted = df.sort_values(by='HourlyDryBulbTemperature', ascending=False)
    print(df_sorted.head())

  
    
               
            



if __name__ == "__main__":
    main()
