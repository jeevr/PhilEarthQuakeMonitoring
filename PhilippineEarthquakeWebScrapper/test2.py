import requests
from bs4 import BeautifulSoup
import re

# URL of the webpage you want to scrape
url = "https://earthquake.phivolcs.dost.gov.ph/2024_Earthquake_Information/October/2024_1001_2119_B4F.html"

# Send a request to fetch the content of the webpage, disabling SSL verification
response = requests.get(url, verify=False)  # 'verify=False' disables SSL certificate check

# If the request is successful (status code 200)
if response.status_code == 200:
    # Parse the content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract the text from the page
    text_content = soup.get_text(separator="\n")  # Use newline as a separator for better readability
    
    # Use regular expression to replace multiple whitespace characters (spaces, newlines, tabs) with a single space
    cleaned_text = re.sub(r'\s+', ' ', text_content).strip()

    # Print or save the text content
    print(cleaned_text)
    
    # Optionally save the text to a file
    with open("earthquake_information.txt", "w", encoding="utf-8") as file:
        file.write(cleaned_text)
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")