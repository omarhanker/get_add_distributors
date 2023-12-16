import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time

# Function to process each page
def process_page(soup):
    """
    Extracts data from a BeautifulSoup soup object representing a web page.
    
    Args:
        soup (BeautifulSoup): The BeautifulSoup soup object representing the web page.
    
    Returns:
        list: A list of dictionaries representing the extracted table data. Each dictionary contains the header values as keys and the corresponding row values as values.
        
    Note:
        - The function first finds the 'div' element with class 'info' in the soup object.
        - If the 'div' element is found, the function then finds the 'table' element within the 'div'.
        - If the 'table' element is found, the function extracts the header values from the first row of the table.
        - The function then iterates through each row of the table (excluding the header row) and extracts the row values.
        - The row values are then used to create a dictionary with the header values as keys.
        - Each dictionary is appended to a list, and the list is returned as the final result.
        - If the 'div' element or the 'table' element is not found, an empty list is returned.
    """
    div_info = soup.find('div', class_='info')
    if div_info:
        table = div_info.find('table')
        if table:
            headers = [header.text for header in table.find('tr').find_all('th')]
            table_data = []
            for row in table.find_all('tr')[1:]:  # Skip the header row
                row_data = [datum.text for datum in row.find_all('td')]
                if row_data:
                    entry = dict(zip(headers, row_data))
                    table_data.append(entry)
            return table_data
    return []

def get_add_distributors():
    """
        Scrape and retrieve a list of NABP-Accredited Drug Distributors (ADD) from a website.
        This function performs the following steps:
        1. Scrapes data from multiple pages of a website.
        2. Processes the scraped data.
        3. Converts the data to JSON format.
        4. Saves the JSON data to a file.
        5. Converts the JSON data to a Pandas DataFrame.
        6. Saves the DataFrame as a CSV file.
        7. Prints the scraping completion message and the number of ADDs found.
        
        Returns:
            json_data (str): The scraped data in JSON format.
    """
    # Main scraping logic
    all_data = []
    page = 0

    while True:
        print(f"Processing page {page+1}")
        current_url = f"https://cv.nabp.net/cvweb2/cgi-bin/utilities.dll/CustomList?ORGNAME_field=&STATECD_field=&ORGNAME=&STATECD=&SQLNAME=DIR_VAWD&RANGE={page}1%2F10&sort=ORGNAME&showall=Y&wbp=VAWDList.htm&whp=VAWDheader.htm&wmt=main_template_vawd.htm&SHOWSQL=N"
        response = requests.get(current_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            div_info = soup.find('div', class_='info')

            if div_info and div_info.find('table') and div_info.find('table').find_all('td'):
                page_data = process_page(soup)
                all_data.extend(page_data)
                
            else:
                break
        else:
            print(f"Failed to retrieve page {page+1}")
            break

        page += 1
        time.sleep(0.5)  # Respectful scraping

    # Convert the data to JSON format
    json_data = json.dumps(all_data, indent=3)

    # Save the JSON data to a file
    with open("vwad_list.json", "w") as file:
        file.write(json_data)

    # Convert to Pandas DataFrame and save as CSV
    pd_data = pd.read_json(json_data)
    pd_data.to_csv('vwad_list.csv', index=False, header=True)

    print("Scraping complete. Data saved.")
    print(f"Number of NABP-Accredited Drug Distributors(ADD): {len(all_data)}")
    return json_data

get_add_distributors()