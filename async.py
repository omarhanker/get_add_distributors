import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import pandas as pd

async def fetch_page(session, url):
    """
    Fetches a web page using an HTTP GET request.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        url (str): The URL of the web page to fetch.

    Returns:
        str: The text content of the web page.

    Raises:
        aiohttp.ClientError: If an error occurs during the HTTP request.
    """
    async with session.get(url) as response:
        encoding = 'ISO-8859-1'
        return await response.text(encoding=encoding)

# Function to process each page
def process_page(soup):
    """
    Process the given BeautifulSoup object to extract data from a web page table.
    
    Parameters:
        soup (BeautifulSoup): The BeautifulSoup object representing the web page.
    
    Returns:
        list: A list of dictionaries, where each dictionary represents a row in the table. The keys are the headers of the table columns, and the values are the data in each cell of the row.
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

async def get_add_distributors():
    """
    Retrieves a list of NABP-Accredited Drug Distributors (ADD) from the specified URL, processes the data, and saves it in JSON and CSV formats.

    Returns:
        str: The data in JSON format.
    """
    all_data = []
    page = 0
    async with aiohttp.ClientSession() as session:
        tasks = []
        while True:
            print(f"Processing page {page+1}")
            current_url = f"https://cv.nabp.net/cvweb2/cgi-bin/utilities.dll/CustomList?ORGNAME_field=&STATECD_field=&ORGNAME=&STATECD=&SQLNAME=DIR_VAWD&RANGE={page}1%2F10&sort=ORGNAME&showall=Y&wbp=VAWDList.htm&whp=VAWDheader.htm&wmt=main_template_vawd.htm&SHOWSQL=N"
            page_content = await fetch_page(session, current_url)
            soup = BeautifulSoup(page_content, 'html.parser')
            div_info = soup.find('div', class_='info')

            if not (div_info and div_info.find('table') and div_info.find('table').find_all('td')):
                break

            tasks.append(asyncio.create_task(fetch_page(session, current_url)))
            page += 1

        responses = await asyncio.gather(*tasks)

        for response in responses:
            soup = BeautifulSoup(response, 'html.parser')
            div_info = soup.find('div', class_='info')
            if div_info and div_info.find('table') and div_info.find('table').find_all('td'):
                page_data = process_page(soup)
                all_data.extend(page_data)
            else:
                break

        # Convert the data to JSON format and save
        json_data = json.dumps(all_data, indent=3)
        with open("vwad_list.json", "w") as file:
            file.write(json_data)

        # Convert to Pandas DataFrame and save as CSV
        pd_data = pd.read_json(json_data)
        pd_data.to_csv('vwad_list.csv', index=False, header=True)

        print("Scraping complete. Data saved.")
        print(f"Number of NABP-Accredited Drug Distributors(ADD): {len(all_data)}")
        return json_data

# To run the async function
asyncio.run(get_add_distributors())
