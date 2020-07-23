# Scratch work
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


# Using lists (going to switch to class I think)
from google.cloud import bigquery

destination_client = bigquery.Client()
public_client = bigquery.Client(project='bigquery-public-data')

# For destination tables
destination_dataset_id = 'covid_sources'
destination_tables_to_check = ['usafacts_confirmed_cases', 'usafacts_deaths']

# For source tables
source_dataset_id = 'covid19_usafacts'
source_tables_to_check = ['confirmed_cases', 'deaths']

def get_tables_from_dataset(dataset_id, table_names, client):
    """
    Uses dataset id, list of table names to get BQ table object
    """
    dataset = client.get_dataset(dataset_id)
    table_list = []
    for table in table_names:
        table_ref = dataset.table(table)
        table_list.append(client.get_table(table_ref))

    return table_list

destination_tables = get_tables_from_dataset(
                        destination_dataset_id,
                        destination_tables_to_check,
                        destination_client)

source_tables = get_tables_from_dataset(
                    source_dataset_id,
                    source_tables_to_check,
                    public_client)

tables_to_update = []

for dest, source in zip(destination_tables, source_tables):
    print('dest:', dest.modified, ' source:', source.modified)
    if dest.modified < source.modified:
        tables_to_update.append

def compare_table_dates(first_table, second_table):
    if first_table > second_table:
        return True

for table
test_fact_date = usafacts_tables[0].modified
test_dest_date = destination_tables[0].modified

test_fact_date > test_dest_date
# returns true as expected
