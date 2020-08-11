import requests
import pandas as pd
from bs4 import BeautifulSoup

us_vehicle_url = 'https://en.wikipedia.org/wiki/Motor_vehicle_fatality_rate_in_U.S._by_year'

response = requests.get(us_vehicle_url)

soup = BeautifulSoup(response.text, 'lxml')

tables = soup.find_all('table')

df_list = pd.read_html(str(tables))

us_vehicle_df = df_list[0][['Year', 'Deaths']].copy()



print(cdc_deaths_df.dtypes)
print(cdc_deaths_df)
