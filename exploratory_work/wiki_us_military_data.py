import requests
import pandas as pd
from bs4 import BeautifulSoup

us_war_url = 'https://en.wikipedia.org/wiki/United_States_military_casualties_of_war'

response = requests.get(us_war_url)

soup = BeautifulSoup(response.text, 'lxml')

tables = soup.find_all('table')

df = pd.read_html(str(table))

# df[3] is what I'm looking for
