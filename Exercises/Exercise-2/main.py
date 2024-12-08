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
    print(f'Reading dataframe with {len(df)} rows')
    return df


def main():
    # your code here
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    urls_list = get_download_urls(base_url=base_url)

    first_url = urls_list[0]
    df = pd.read_csv(first_url)
    df_sorted = df.sort_values(by='HourlyDryBulbTemperature', ascending=False)
    print(df_sorted)

  
    
               
            



if __name__ == "__main__":
    main()
