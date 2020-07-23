
from google.cloud import bigquery

# These can move to environment variables when deployed
destination_project_id = 'big-query-horse-play'
destination_dataset_id = 'covid_sources'
destination_tables_to_check = ['usafacts_confirmed_cases', 'usafacts_deaths']

source_project_id = 'bigquery-public-data'
source_dataset_id = 'covid19_usafacts'
source_tables_to_check = ['confirmed_cases', 'deaths']

class DataSource:
    """
    Uses destination and source identifiers to construct object for updating
    data sources.
    """
    def __init__(self, dest_project, dest_dataset, dest_table,
                source_project, source_dataset, source_table):
        """
        Args:
            dest_project: Unqualified destination GCP Project id
            dest_dataset: Unqualified destination Dataset id
            dest_table: Unqualified destination Table name
            source_project: Unqualified source GCP Project id
            source_dataset: Unqualified source Dataset id
            source_table: Unqualified source Table name
        """
        self.destination_project_id = dest_project
        self.destination_dataset_id = dest_dataset
        self.destination_table_name = dest_table
        self.destination_client = self.get_client(project_id=self.destination_project_id)
        self.destination_dataset = self.get_dataset(self.destination_client,
                                                    self.destination_dataset_id)
        self.destination_table = self.get_table(self.destination_client,
                                                self.destination_dataset,
                                                self.destination_table_name)

        self.source_project_id = source_project
        self.source_dataset_id = source_dataset
        self.source_table_name = source_table
        self.source_client = self.get_client(project_id=self.source_project_id)
        self.source_dataset = self.get_dataset(self.source_client, self.source_dataset_id)
        self.source_table = self.get_table(self.source_client,
                                        self.source_dataset,
                                        self.source_table_name)

        self.update = self.update_required(self.destination_table, self.source_table)

    # Attaching these here for invoking on init
    def get_client(self, project_id):
        client_name = bigquery.Client(project=project_id)
        return client_name

    def get_dataset(self, client, dataset_id):
        dataset = client.get_dataset(dataset_id)
        return dataset

    def get_table(self, client, dataset, table_name):
        table_ref = dataset.table(table_name)
        table = client.get_table(table_ref)
        return table

    def update_required(self, destination_table, source_table):
        if destination_table.modified < source_table.modified:
            return True
        else:
            return False


query = f'SELECT * FROM `{project}`.`{dataset}`.`{table}`'
def update_data_source(data_source):
    if data_source.update is True:


cases = DataSource(
                destination_project_id,
                destination_dataset_id,
                destination_tables_to_check[0],
                source_project_id,
                source_dataset_id,
                source_tables_to_check[0])

deaths = DataSource(
                destination_project_id,
                destination_dataset_id,
                destination_tables_to_check[1],
                source_project_id,
                source_dataset_id,
                source_tables_to_check[1])
