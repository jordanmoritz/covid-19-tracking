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

destination_tables = get_tables_from_dataset(destination_dataset, destination_tables_to_check, client)
source_tables = get_tables_from_dataset(usafacts_dataset, usafacts_tables_to_check, public_client)

def compare_table_dates(first_table, second_table):
    if first_table > second_table:
        return True



for table
test_fact_date = usafacts_tables[0].modified
test_dest_date = destination_tables[0].modified

test_fact_date > test_dest_date
# returns true as expected
