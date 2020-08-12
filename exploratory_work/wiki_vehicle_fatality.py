import requests
import pandas as pd
from bs4 import BeautifulSoup

us_vehicle_url = 'https://en.wikipedia.org/wiki/Motor_vehicle_fatality_rate_in_U.S._by_year'

response = requests.get(us_vehicle_url)

soup = BeautifulSoup(response.text, 'lxml')

tables = soup.find_all('table')

df_list = pd.read_html(str(tables))

us_vehicle_df = df_list[0][['Year', 'Deaths']].copy()

# Rename columns
us_vehicle_df.rename(columns={'Year': 'year',
                        'Deaths': 'total_deaths'},
                inplace=True)

us_vehicle_df = us_vehicle_df.apply(lambda x: x.str.strip(), axis='columns')

# Clean year formatting
us_vehicle_df['year'] = us_vehicle_df['year'].apply(lambda x: x[:4])

# Fix issue with death data
us_vehicle_df.iat[0, 1] = us_vehicle_df.iat[0, 1][:2]

# Cast data types
us_vehicle_df = us_vehicle_df.astype({'total_deaths': 'int64'})

print(us_vehicle_df.dtypes)
print(us_vehicle_df)
print(us_vehicle_df.total_deaths.unique())
print(us_vehicle_df.year.unique())
