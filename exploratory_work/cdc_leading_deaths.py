import requests
import pandas as pd
from bs4 import BeautifulSoup

cdc_deaths_url = 'https://www.cdc.gov/nchs/fastats/leading-causes-of-death.htm'

response = requests.get(cdc_deaths_url)

soup = BeautifulSoup(response.text, 'lxml')

ul_list = soup.find('ul', class_='list-false')

li_member_list = []
for li in ul_list.findAll('li'):
    li_member = li.text.split(':')
    li_member_list.append(li_member)

cdc_deaths_df = pd.DataFrame(data=li_member_list,
                    columns=['cause_of_death', 'total_deaths'])

cdc_deaths_df['total_deaths'] = cdc_deaths_df['total_deaths'].apply(lambda x: x.strip().replace(',', ''))

# Cast data types
cdc_deaths_df = cdc_deaths_df.astype({'total_deaths': 'int64'})

print(cdc_deaths_df.dtypes)
print(cdc_deaths_df)
