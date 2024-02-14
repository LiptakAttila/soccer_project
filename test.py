from datetime import datetime
import pandas as pd
import json
from geopy import Nominatim
import os
from dotenv import load_dotenv

load_dotenv()


credential = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

from google.cloud import bigquery

urls = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
no_image = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests
    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.text

    except requests.RequestException as e:
        print(f"an error occured: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')

    return table_rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(url):
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text.replace(',', '').replace('.', '')),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': "https://" + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text)

        }
        data.append(values)
    df = pd.DataFrame(data)
    df.to_csv('data/output.csv', index=False)
    json_data = json.dumps(data)

    return json_data


def get_lat_long(country, city):
    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(data):
    json_data = json.loads(data)

    df = pd.DataFrame(json_data)

    df['images'] = df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else no_image)
    df['capacity'] = df['capacity'].astype(int)
    # df['location'] = df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    #
    # #duplicates
    #
    # duplicates = df[df.duplicated(['location'])]
    # duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    # df.update(duplicates)
    return df


def write_wikipedia_data(data):
    from google.oauth2 import service_account
    from pandas_gbq import to_gbq

    file_name = ('stadium_cleaned_' + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(":",
                                                                                                            "_") + '.csv')
    credentials = service_account.Credentials.from_service_account_file(credential)

    project_id = 'dataengineers-407810'

    table_id = 'dataengineers-407810.football_project_dataset.football_table'

    to_gbq(data, table_id, project_id=project_id, credentials=credentials, if_exists='replace')

write_wikipedia_data(transform_wikipedia_data(extract_wikipedia_data(urls)))
