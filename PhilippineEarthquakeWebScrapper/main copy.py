import os
import selenium.webdriver as webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time
import pandas as pd


def initialize_scrapper(url):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0'
    edge_driver_path = os.path.join(os.getcwd(), 'edgedriver_win64', 'msedgedriver.exe')
    edge_service = Service(edge_driver_path)
    edge_options = Options()
    edge_options.add_argument(f'user-agent={user_agent}')

    browser = webdriver.Edge(service=edge_service, options=edge_options)
    browser.get(url)

    print("Page loaded")
    time.sleep(2) # give time to load the page

    return browser


def scrape_data(browser):
    tbody = browser.find_element(By.XPATH, '/html/body/div/table[3]/tbody') # xpath of specfic table in webpage

    data = []

    for tr in tbody.find_elements(By.XPATH, '//tr'):
        row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
        data.append(row)

    return data

def clean_data(scraped_data):
    # print(scraped_data)

    # ====================================
    # get key headers of the data
    # ====================================
    data_month_year = scraped_data[1][0]
    
    data_month, data_year = data_month_year.split(' ')
    
    print(data_month)
    print(data_year)

    data_removed_empty = [entry for entry in scraped_data if entry]  # Remove empty lists

    # ====================================
    # find where the actual data ended
    # ====================================
    idx_row = 0
    end_row = 0
    for row in data_removed_empty:
        print(row)
        if row[0] == data_year:
            end_row = idx_row
        
        idx_row += 1

    # print(end_row)

    # ====================================
    # apply final filtering of array
    # ====================================

    final_array = data_removed_empty[2:end_row] # 2 - start value based on the actual data
    # print(final_array)

    # Define headers
    df_headers = ['date_and_time', 'latitude', 'longitude', 'depth_km', 'magnitude', 'location']

    # Convert the sliced list to a DataFrame
    df = pd.DataFrame(final_array, columns=df_headers)

    # Print the DataFrame
    # print(df)

    # Save the DataFrame to a CSV file
    csv_file_path = f'earthquake_data_{data_month.lower()}_{data_year.lower()}.csv'
    df.to_csv(csv_file_path, index=False)

    # Print confirmation
    print(f"DataFrame saved to {csv_file_path}")



if __name__ == '__main__':

    url = 'https://earthquake.phivolcs.dost.gov.ph/'
    url2 = 'https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/2024/2024_September.html'

    # browser = initialize_scrapper(url)
    # scraped_data = scrape_data(browser)

    # print(scraped_data)

    scraped_data = [
        ['PHIVOLCS LATEST EARTHQUAKE INFORMATION'],
        ['OCTOBER 2024'],
        [],
        ['05 October 2024 - 03:53 PM', '10.12', '126.52', '012', '3.4', '051 km N 77° E of Burgos (Surigao Del Norte)'],
        ['05 October 2024 - 03:46 PM', '10.06', '126.72', '008', '4.2', '071 km N 87° E of Burgos (Surigao Del Norte)'],
        ['05 October 2024 - 03:37 PM', '09.91', '122.30', '016', '1.8', '021 km N 44° W of City Of Sipalay (Negros Occidental)'],
        ['05 October 2024 - 02:24 PM', '18.56', '120.98', '022', '1.9', '002 km West of Santa Praxedes (Cagayan)'],
        ['05 October 2024 - 01:10 PM', '09.77', '126.93', '007', '3.1', '085 km S 89° E of General Luna (Surigao Del Norte)'],
        ['05 October 2024 - 10:06 AM', '11.73', '125.24', '041', '1.8', '020 km N 81° E of Hinabangan (Samar)'],
        ['05 October 2024 - 09:50 AM', '07.79', '126.93', '013', '3.5', '048 km N 60° E of Baganga (Davao Oriental)'],
        ['2024'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'],
        ['2023'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        ['2022'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        ['2021'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        ['2020'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        ['2019'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        ['2018'],
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    ]

    clean_data(scraped_data)