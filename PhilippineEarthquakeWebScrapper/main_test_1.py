import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Step 1: Fetch the webpage with SSL verification disabled
url = "https://earthquake.phivolcs.dost.gov.ph/"
response = requests.get(url, verify=False)

# Step 2: Parse the webpage content
soup = BeautifulSoup(response.content, 'html.parser')

print(soup.prettify())

# Step 3: Locate the table
table = soup.find('table')

print(table)

# Step 4: Extract headers
headers = [header.text.strip() for header in table.find_all('th')]

# Step 5: Extract rows
rows = []
for row in table.find_all('tr')[1:]:
    cols = [col.text.strip() for col in row.find_all('td')]
    rows.append(cols)

# Step 6: Convert to pandas DataFrame
df = pd.DataFrame(rows, columns=headers)

# Step 7: Save to CSV
df.to_csv('earthquake_data.csv', index=False)

print("Data saved to earthquake_data.csv")
