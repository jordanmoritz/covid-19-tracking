import requests
import pandas as pd
from bs4 import BeautifulSoup

cdc_influenza_url = 'https://www.cdc.gov/flu/about/burden/index.html'

response = requests.get(cdc_influenza_url)

soup = BeautifulSoup(response.text, 'lxml')

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

print(cdc_infl_df.dtypes)
print(cdc_infl_df)
