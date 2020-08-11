import requests
import pandas as pd
from bs4 import BeautifulSoup

us_war_url = 'https://en.wikipedia.org/wiki/United_States_military_casualties_of_war'

response = requests.get(us_war_url)

soup = BeautifulSoup(response.text, 'lxml')

tables = soup.find_all('table')

df_list = pd.read_html(str(tables))

# df[1] is what I'm looking for

us_war_df = df_list[1].iloc[:, [1, 3]].copy()

us_war_df.describe()

# us_war_casualties_df.drop(0, axis='index')

us_war_df.dtypes

# Fix malformed value for civil war
us_war_df.iat[0, 1] = us_war_df.iat[0, 1][:7].replace(',', '')

us_war_df.astype({'Deaths': 'int64'}).dtypes

us_war_df.rename(columns={'War': 'war_name',
                        'Deaths': 'total_deaths'},
                inplace=True)
