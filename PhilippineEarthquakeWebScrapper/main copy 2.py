import os
import selenium.webdriver as webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import time
import pandas as pd
import logging
from datetime import datetime


class Logger:
    def __init__(self, log_file='logs/scraper_log.txt'):
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the overall logging level

        # Create log file with dynamic name based on date and time
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f'{log_file}_{current_time}.txt'

        # Create handlers
        file_handler = logging.FileHandler(log_file_name)
        console_handler = logging.StreamHandler()

        # Set log level for handlers
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.INFO)

        # Create formatters and add them to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def log_message(self, message, level='info'):
        """Logs messages to the log file with specified logging level."""
        if level == 'info':
            self.logger.info(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'exception':
            self.logger.exception(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'debug':
            self.logger.debug(message)
        else:
            self.logger.info(message)  # Default to info


def initialize_scrapper(url, logger):
    try:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0'
        edge_driver_path = os.path.join(os.getcwd(), 'edgedriver_win64', 'msedgedriver.exe')
        edge_service = Service(edge_driver_path)
        edge_options = Options()
        edge_options.add_argument(f'user-agent={user_agent}')

        browser = webdriver.Edge(service=edge_service, options=edge_options)
        browser.get(url)

        logger.log_message("Page loaded successfully", level='info')
        time.sleep(2)  # give time to load the page

        return browser

    except Exception as e:
        logger.log_message("Failed to initialize scraper", level='exception')
        return None


def scrape_data(browser, logger):
    try:
        tbody = browser.find_element(By.XPATH, '/html/body/div/table[3]/tbody')  # xpath of specific table in webpage

        data = []

        for tr in tbody.find_elements(By.XPATH, '//tr'):
            row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
            data.append(row)

        logger.log_message("Data scraped successfully", level='info')
        return data

    except Exception as e:
        logger.log_message("Failed to scrape data", level='exception')
        return []


def clean_data(scraped_data, logger):
    try:
        # ====================================
        # get key headers of the data
        # ====================================
        data_month_year = scraped_data[1][0]

        # Ensure there's a space to split on
        data_month_year_parts = data_month_year.split(' ')
        
        data_month, data_year = data_month_year_parts

        logger.log_message(f"Month: {data_month}", level='debug')
        logger.log_message(f"Year: {data_year}", level='debug')

        data_removed_empty = [entry for entry in scraped_data if entry]  # Remove empty lists

        # ====================================
        # find where the actual data ended
        # ====================================
        idx_row = 0
        end_row = 0
        for row in data_removed_empty:
            if row[0] == data_year:
                end_row = idx_row
            
            idx_row += 1

        # ====================================
        # apply final filtering of array
        # ====================================

        final_array = data_removed_empty[2:end_row]  # 2 - start value based on the actual data

        # Define headers
        df_headers = ['date_and_time', 'latitude', 'longitude', 'depth_km', 'magnitude', 'location']

        # Convert the sliced list to a DataFrame
        df = pd.DataFrame(final_array, columns=df_headers)

        # Save the DataFrame to a CSV file
        csv_file_path = f'scraped_data/earthquake_data_{data_month.lower()}_{data_year.lower()}.csv'
        df.to_csv(csv_file_path, index=False)

        # Log confirmation
        logger.log_message(f"DataFrame saved to {csv_file_path}", level='info')

    except Exception as e:
        logger.log_message("Failed to clean data", level='exception')


if __name__ == '__main__':
    url = 'https://earthquake.phivolcs.dost.gov.ph/'
    url2 = 'https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/2024/2024_September.html'

    logger = Logger()  # Initialize the logger instance

    browser = initialize_scrapper(url, logger)
    scraped_data = scrape_data(browser, logger)

    # scraped_data = [
    #     ['PHIVOLCS LATEST EARTHQUAKE INFORMATION'],
    #     ['OCTOBER 2024'],
    #     [],
    #     ['05 October 2024 - 03:53 PM', '10.12', '126.52', '012', '3.4', '051 km N 77° E of Burgos (Surigao Del Norte)'],
    #     ['05 October 2024 - 03:46 PM', '10.06', '126.72', '008', '4.2', '071 km N 87° E of Burgos (Surigao Del Norte)'],
    #     ['05 October 2024 - 03:37 PM', '09.91', '122.30', '016', '1.8', '021 km N 44° W of City Of Sipalay (Negros Occidental)'],
    #     ['05 October 2024 - 02:24 PM', '18.56', '120.98', '022', '1.9', '002 km West of Santa Praxedes (Cagayan)'],
    #     ['05 October 2024 - 01:10 PM', '09.77', '126.93', '007', '3.1', '085 km S 89° E of General Luna (Surigao Del Norte)'],
    #     ['05 October 2024 - 10:06 AM', '11.73', '125.24', '041', '1.8', '020 km N 81° E of Hinabangan (Samar)'],
    #     ['05 October 2024 - 09:50 AM', '07.79', '126.93', '013', '3.5', '048 km N 60° E of Baganga (Davao Oriental)'],
    #     ['2024'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'],
    #     ['2023'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #     ['2022'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #     ['2021'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #     ['2020'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #     ['2019'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #     ['2018'],
    #     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    # ]

    clean_data(scraped_data, logger)
