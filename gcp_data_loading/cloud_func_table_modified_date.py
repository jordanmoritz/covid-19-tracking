from google.cloud import bigquery

client = bigquery.Client()
public_client = bigquery.Client(project='bigquery-public-data')

dataset_id = 'covid19_usafacts'

dataset = public_client.get_dataset(dataset_id)

tables_to_check = ['confirmed_cases', 'deaths']
tables = []
for table in tables_to_check:
    table_ref = dataset.table(table)
    tables.append(public_client.get_table(table_ref))

print(tables)

for table in tables:
    print('modified', table.modified, sep=':')
