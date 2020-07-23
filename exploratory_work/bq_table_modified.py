from google.cloud import bigquery

client = bigquery.Client()
public_client = bigquery.Client(project='bigquery-public-data')

usafacts_dataset_id = 'covid19_usafacts'
destination_dataset_id = 'covid_sources'

usafacts_dataset = public_client.get_dataset(usafacts_dataset_id)
destination_dataset = client.get_dataset(destination_dataset_id)



usafacts_tables_to_check = ['confirmed_cases', 'deaths']
destination_tables_to_check = ['usafacts_confirmed_cases', 'usafacts_deaths']

def get_tables_from_dataset(dataset, tables, client):
    table_list = []
    for table in tables:
        table_ref = dataset.table(table)
        table_list.append(client.get_table(table_ref))

    return table_list

usafacts_tables = get_tables_from_dataset(usafacts_dataset, usafacts_tables_to_check, public_client)

print('usafacts tables:', usafacts_tables)

for table in usafacts_tables:
    print('modified', table.modified, sep=':')

destination_tables = get_tables_from_dataset(destination_dataset, destination_tables_to_check, client)

print('dest tables:', destination_tables)

test_fact_date = usafacts_tables[0].modified
test_dest_date = destination_tables[0].modified

test_fact_date > test_dest_date
# returns true as expected
