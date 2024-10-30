import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import hashlib

webpage_url = 'https://meteo.gov.lk/index.php?lang=en'
metdata_folder = 'metdata'

def get_daily_pdf_link(webpage_url):
    try:
        response = requests.get(webpage_url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        specific_li = soup.find('li', {'data-id': '567', 'data-level': '2'})
        if specific_li:
            specific_link = specific_li.find('a', href=True)
            if specific_link:
                daily_link = specific_link['href']
                if not daily_link.startswith('http'):
                    daily_link = requests.compat.urljoin(webpage_url, daily_link)
                return daily_link

        print("Specific link not found.")
        return None

    except requests.RequestException as e:
        print(f"An error occurred while fetching the webpage: {e}")
        return None

def hash_file(filepath):
#Generate SHA256 hash of a file
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def download_pdf(pdf_url):
    try:
        response = requests.get(pdf_url, verify=False)
        response.raise_for_status()

        date_string = datetime.now().strftime('%Y-%m-%d')
        local_filename = f'{metdata_folder}/daily_climate_update_{date_string}.pdf'

        # Check if the file already exists
        if os.path.exists(local_filename):
            print(f"PDF for {date_string} already exists.")
            return local_filename

        # Download the new PDF
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        with open(local_filename, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded PDF: {local_filename}")
        return local_filename

    except requests.RequestException as e:
        print(f"An error occurred while downloading the PDF: {e}")
        return None

def check_if_pdf_is_new(pdf_url):
    try:
        # Download the PDF into a temporary file for comparison
        response = requests.get(pdf_url, verify=False)
        response.raise_for_status()

        date_string = datetime.now().strftime('%Y-%m-%d')
        temp_filename = f'{metdata_folder}/temp_{date_string}.pdf'

        # Save the temporary PDF
        with open(temp_filename, 'wb') as file:
            file.write(response.content)

        # Check if there is a previous file to compare
        previous_pdf_files = sorted(
            [f for f in os.listdir(metdata_folder) if f.startswith('daily_climate_update')],
            reverse=True
        )
        if previous_pdf_files:
            latest_pdf_path = os.path.join(metdata_folder, previous_pdf_files[0])
            if hash_file(latest_pdf_path) == hash_file(temp_filename):
                print(f"PDF for today is the same as {previous_pdf_files[0]}. No download needed.")
                os.remove(temp_filename)  # Clean up the temporary file
                return latest_pdf_path  # Return the path of the existing file

        # If PDF is new, move the temp file to today's file
        new_pdf_path = f'{metdata_folder}/daily_climate_update_{date_string}.pdf'
        os.rename(temp_filename, new_pdf_path)
        return new_pdf_path

    except requests.RequestException as e:
        print(f"An error occurred during PDF comparison: {e}")
        return None

if __name__ == "__main__":
    # Get the PDF link
    pdf_url = get_daily_pdf_link(webpage_url)

    if pdf_url:
        pdf_path = check_if_pdf_is_new(pdf_url)
        if pdf_path:
            print(f"PDF for today is available at {pdf_path}")
        else:
            print("Failed to get PDF or PDF is unchanged.")
    else:
        print("Failed to get PDF link.")
