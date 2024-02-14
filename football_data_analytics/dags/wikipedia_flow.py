import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pip._internal.network.session import user_agent

# from ..pipelines.wikipedia_pipeline import get_wikipedia_page

no_image = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Liptak Attila",
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval=None,
    catchup=False
)


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


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')

        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text)

        }
        data.append(values)

    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"


def get_lat_long(country, city):
    from geopy import Nominatim

    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadium_df = pd.DataFrame(data)

    # stadium_df['location'] = stadium_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadium_df['images'] = stadium_df['images'].apply(lambda x: x if x != x not in ['NO_IMAGE', '', None] else no_image)
    stadium_df['capacity'] = stadium_df['capacity'].astype(int)

    # duplicates
    # duplicates = stadium_df.duplicated(['location'])
    # duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    # stadium_df.update(duplicates)

    # push to xcom

    kwargs['ti'].xcom_push(key='rows', value=stadium_df.to_json())


def write_wikipedia_data(**kwargs):
    from google.oauth2 import service_account
    from pandas_gbq import to_gbq

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = (
                 'stadium_cleaned_'
                 + str(datetime.now().date()) + "_"
                 + str(datetime.now().time()).replace(":",",")
                 + '.csv'
    )

    credentials = service_account.Credentials.from_service_account_file("""
      "type": "service_account",
      "project_id": "dataengineers-407810",
      "private_key_id": "76cde645b3d26f703e9a6f7a72e3f0ed295a722e",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCZrNUKpV6emw+J\nY6Gldn1akcX1p9mJAfu4XQgpBE0C4SasQOeF6SepgbJL28xRydnkMyIgOdFF2j7T\n7EHnuWx9TeiW8fWmWhNAJUFIrbIOOLpkiqxzEsPhV9zSeAKUxT3GAW4RXf2TEbkU\nf6VoK3SaB0/jk7WPuZeifR6Qu5aV6vnH3qsAHSLpIGYxh1XhwBvc3DAD784hil+8\nXTAnxuRKZ9PAeiuvmAtqv1eXh11yRkkwezEdI63V0w0MuhTS8WT9kdGljWOzpr8g\nJn9Iz4dHDbBMxBwk0OhffdBbppehtL3YSIzVjFIzMoQSaTWu3Ptfn5uigJnIJ/4h\n3eM5ZJkZAgMBAAECggEAD+Zz6wxx+jFjRcT9H0p/nRi+3aRis8Q0pC5dByhqovEC\nkDlNczus5sMUfHiXHwHGdY/sWJ7+1g5HKKR167jR3rOqTH7nurDwc7c/FlrhdMr+\nDyPRhL0E7yqI/q5b3wJ6+iMg/TNHRrmjV5l+9uSjJYI66/o3I5IxfCabvGAqbCOp\nLII3gr8PPWfSY1Q3wIwgpzX9pCSbWAUbhZ5nxtX/bmH7J97PANm2KTXQ3JZJXc3y\nOQHeE1aEA24LDs4dZffp8e39DTYYxrgBDtPzTKdVvk2le3nky1Btn/y/7iUrTnfR\nRTa8g1yGXq/jk6CoSzXlNn0fIuwjn+X1bi4wt+aKMQKBgQDJcr+M5bNPL3k0O+2W\nDP3cyM9KCiO+b0n8wZn0YSuMw7yd5H9MuCqM6Ue3+gSy9bk2K4LVE52ZMqne9htb\n/7O7nKCZUQcJWDhgfhElYLn89pVatetAnIzoVtO9Z1N/3AFHe9Jm1br3TQQ2gpaX\ni4ZsG/EqwW/77B5v/LYFpc/40QKBgQDDSj9+txFbigGVjmEVWKiy4g9F35ZF26Pt\nGvpzmVlkHKDCvu9WX773GOV6Xa2hgxDcQpWynwhW8Ys8L3vCb1fsGp/o4pWr+13q\n7QEf85DEGseYjdyeMb2vk1ApwHsc9Un9WyxFW+32mrijx7eNNyWPDm/MgmNkwGm4\nHxno++WtyQKBgG5KldclV7Ot8TUI7n4tpLvBccQ+ou07bw2kTY7/uGE8NUNY+91c\nI2e8vNhcWUBFyl5/GgVg1PMwStLYeYg4CgZYrZDjTO/vzgX+X5LQT2PQ+VqNqTxZ\nucfm6cyUCqJBKK4je8FzmQaWswzJGHvmFvWPos1e1Nnfn5Z+jIWxD5lRAoGBALYx\nkOdRN9pk+HFk8eIOYuTwzjrsC7CE53yPzNUIXkNuCfVmWLgGf4zcJ0twFks2kgZM\nAIkyoX8rvjCaRCXz5t0ZQdLtgaF/0/EWyBPdElRCf7AtuYPE+CjTkNhDARjVJwA9\njSHwUCPf2R7DdtVCe8HpQBhWFRCsCnWCXpPkXjUxAoGBAIx5MaRs2gng/3zo5BAi\nVEh2+cPZZxFqH8Fj/6DgHs2nUbFUBmZJ7SgaC4zhCKyZhB2ppOxwgQVhWoQd6b8W\nXPhnMskQKpNunO6LunjVgoCmnZUGJG735rTJL7ZDHfTgHbo6fkTKqFwcoJCFpuUM\nBSPiHTJssttfTQIb+RgrJIF2\n-----END PRIVATE KEY-----\n",
      "client_email": "football-sa@dataengineers-407810.iam.gserviceaccount.com",
      "client_id": "111420141736979133087",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/football-sa%40dataengineers-407810.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    """)

    project_id = 'dataengineers-407810'
    table_id = 'dataengineers-407810.football_project_dataset.football_table'

    to_gbq(data, table_id, project_id=project_id, if_exists='replace', credentials=credentials)


# Extraction

extract_data_from_wikipedia = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)
transform_wikipedia_data = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag
)
write_wikipedia_data = PythonOperator(
    task_id="write_wikipedia_data",
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
