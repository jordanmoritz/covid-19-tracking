import requests
import pandas as pd
import pandas_gbq
import os
from bs4 import BeautifulSoup

project_id = os.environ.get('GCP_PROJECT_ID')
destination_dataset_id = os.environ.get('GCP_DEST_DATASET_ID')

us_war = {
    'url': 'https://en.wikipedia.org/wiki/United_States_military_casualties_of_war',
    'destination_table_name': 'us_war_casualties'
    }

us_vehicle = {
    'url': 'https://en.wikipedia.org/wiki/Motor_vehicle_fatality_rate_in_U.S._by_year',
    'destination_table_name': 'us_motor_vehicle_fatalities'
    }

cdc_influenza = {
    'url': 'https://www.cdc.gov/flu/about/burden/index.html',
    'destination_table_name': 'cdc_us_influenza_burden'
    }

cdc_deaths = {
    'url': 'https://www.cdc.gov/nchs/fastats/leading-causes-of-death.htm',
    'destination_table_name': 'cdc_us_leading_causes_death'
    }

def get_soup(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    return soup

def get_war_df(url):
    soup = get_soup(url)
    tables = soup.find_all('table')
    df_list = pd.read_html(str(tables))

    us_war_df = df_list[1][['War', 'Deaths']].copy()

    # Rename columns
    us_war_df.rename(columns={'War': 'war_name',
                            'Deaths': 'total_deaths'},
                    inplace=True)

    # Fix malformed value for civil war
    us_war_df.iat[0, 1] = us_war_df.iat[0, 1][:7].replace(',', '')

    # Cast
    us_war_df = us_war_df.astype({'total_deaths': 'int64'})

    return us_war_df

def get_vehicle_df(url):
    soup = get_soup(url)
    tables = soup.find_all('table')
    df_list = pd.read_html(str(tables))

    us_vehicle_df = df_list[0][['Year', 'Deaths']].copy()

    # Rename columns
    us_vehicle_df.rename(columns={'Year': 'year',
                            'Deaths': 'total_deaths'},
                    inplace=True)

    # Drop whitespace
    us_vehicle_df = us_vehicle_df.apply(lambda x: x.str.strip(), axis='columns')

    # Clean year formatting
    us_vehicle_df['year'] = us_vehicle_df['year'].apply(lambda x: x[:4])

    # Fix issue with death data
    us_vehicle_df.iat[0, 1] = us_vehicle_df.iat[0, 1][:2]

    # Cast
    us_vehicle_df = us_vehicle_df.astype({'total_deaths': 'int64'})

    return us_vehicle_df

def get_cdc_infl_df(url):
    soup = get_soup(url)

    table_text = 'Table 1: Estimated Influenza Disease Burden, by Season — United States, 2010-11 through 2018-19 Influenza Seasons'

    table = soup.find('h3', text=table_text).find_parent('table')

    cdc_infl_df = pd.read_html(str(table))[0]

    # Grab relevant columns
    cdc_infl_df = cdc_infl_df.iloc[:, [0,  7]].copy()

    # Drop unnecessary index
    cdc_infl_df = cdc_infl_df.droplevel(0, axis='columns')

    # Drop header row in index
    cdc_infl_df.drop(7, axis='index', inplace=True)

    # Rename columns
    cdc_infl_df.rename(columns={'Season': 'influenza_season',
                            'Estimate': 'estimated_deaths'},
                    inplace=True)

    # Cast data types
    cdc_infl_df = cdc_infl_df.astype({'estimated_deaths': 'int64'})

    return cdc_infl_df

def get_cdc_deaths_df(url):
    soup = get_soup(url)

    ul_list = soup.find('ul', class_='list-false')

    # Splitting into lists to ingest into df
    li_member_list = []
    for li in ul_list.findAll('li'):
        li_member = li.text.split(':')
        li_member_list.append(li_member)

    # Create df
    cdc_deaths_df = pd.DataFrame(data=li_member_list,
                        columns=['cause_of_death', 'total_deaths'])

    # Clean deaths column
    cdc_deaths_df['total_deaths'] = cdc_deaths_df['total_deaths'].apply(lambda x: x.strip().replace(',', ''))

    # Cast data types
    cdc_deaths_df = cdc_deaths_df.astype({'total_deaths': 'int64'})

    return cdc_deaths_df

# Get dfs
us_war['df'] = get_war_df(us_war.get('url'))
us_vehicle['df'] = get_vehicle_df(us_vehicle.get('url'))
cdc_influenza['df'] = get_cdc_infl_df(cdc_influenza.get('url'))
cdc_deaths['df'] = get_cdc_deaths_df(cdc_deaths.get('url'))

data_to_upload_list = [us_war, us_vehicle, cdc_influenza, cdc_deaths]

# Load 'em in
for data in data_to_upload_list:
    destination_table_id = destination_dataset_id + '.' + data.get('destination_table_name')
    pandas_gbq.to_gbq(data.get('df'), destination_table_id, project_id=project_id)
