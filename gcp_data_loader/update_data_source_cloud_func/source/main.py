import os, json
from google.cloud import bigquery, pubsub_v1

destination_project_id = os.environ.get('DEST_PROJ_ID')
destination_dataset_id = os.environ.get('DEST_DATASET_ID')
destination_tables_to_check = json.loads(os.environ.get('DEST_TABLES_TO_CHECK'))

source_project_id = os.environ.get('SOURCE_PROJ_ID')
source_dataset_id = json.loads(os.environ.get('SOURCE_DATASET_ID'))
source_tables_to_check = json.loads(os.environ.get('SOURCE_TABLES_TO_CHECK'))

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
        self.destination_fully_qualified_table_name = f'{dest_project}.{dest_dataset}.{dest_table}'
        self.destination_client = self.get_client(project_id=self.destination_project_id)
        self.destination_dataset = self.get_dataset(self.destination_client,
                                                    self.destination_dataset_id)

        self.source_project_id = source_project
        self.source_dataset_id = source_dataset
        self.source_table_name = source_table
        self.source_fully_qualified_table_name = f'{source_project}.{source_dataset}.{source_table}'
        self.source_client = self.get_client(project_id=self.source_project_id)
        self.source_dataset = self.get_dataset(self.source_client, self.source_dataset_id)

    # Attaching these here for invoking on init
    def get_client(self, project_id):
        client_name = bigquery.Client(project=project_id)
        return client_name

    def get_dataset(self, client, dataset_id):
        dataset = client.get_dataset(dataset_id)
        return dataset

    # For handling data source updates
    def get_table(self, client, dataset, table_name):
        table_ref = dataset.table(table_name)
        table = client.get_table(table_ref)
        return table

    def check_update_required(self):
        if not self.destination_table:
            return True
        elif self.destination_table.modified < self.source_table.modified or not self.destination_table:
            return True
        else:
            return False

    def update_job_config(self):
        job_config = bigquery.QueryJobConfig(
                        destination=f'{self.destination_fully_qualified_table_name}',
                        allow_large_results=True,
                        use_query_cache=False,
                        write_disposition='WRITE_TRUNCATE')
        return job_config

    def update_data_source(self, job_config):
        query = f'SELECT * FROM `{self.source_fully_qualified_table_name}`'
        query_job = self.destination_client.query(query, job_config=job_config)
        query_job.result()

def post_message_to_topic(message, project_id, topic_id):
    publisher_client = pubsub_v1.PublisherClient()
    topic_path = publisher_client.topic_path(project_id, topic_id)
    message_data = message.encode('utf-8')

    def callback(future):
        message_id = future.result()
        print('Pub/sub message id: ', message_id)

    future = publisher_client.publish(topic_path, data=message_data)
    future.add_done_callback(callback)

def check_data_source_update(data_source_list):
    # Counter used by pubsub message
    data_sources_updated = 0

    # Handling this piece here because of cache behavior of global variables
    for data_source in data_source_list:

        # Simple handling in case table does not currently exist
        try:
            data_source.destination_table = data_source.get_table(
                                                    data_source.destination_client,
                                                    data_source.destination_dataset,
                                                    data_source.destination_table_name)
        except:
            data_source.destination_table = None

        data_source.source_table = data_source.get_table(data_source.source_client,
                                        data_source.source_dataset,
                                        data_source.source_table_name)

        data_source.update_required = data_source.check_update_required()

        if data_source.update_required is True:
            data_source.job_config = data_source.update_job_config()
            data_source.update_data_source(data_source.job_config)
            print('Data Source Updated: ', f'{data_source.destination_fully_qualified_table_name}')
            data_sources_updated += 1
        else:
            print('Data Source not updated, source table not refreshed:\n',
                    f'{data_source.source_fully_qualified_table_name}')

    return data_sources_updated

state_county_cases = DataSource(
                destination_project_id,
                destination_dataset_id,
                destination_tables_to_check[0],
                source_project_id,
                source_dataset_id[0],
                source_tables_to_check[0])

state_county_deaths = DataSource(
                destination_project_id,
                destination_dataset_id,
                destination_tables_to_check[1],
                source_project_id,
                source_dataset_id[0],
                source_tables_to_check[1])

country_daily_volume = DataSource(
                destination_project_id,
                destination_dataset_id,
                destination_tables_to_check[2],
                source_project_id,
                source_dataset_id[1],
                source_tables_to_check[2])

data_source_list = [state_county_cases, state_county_deaths, country_daily_volume]

# In normal circumstances would likely be parsing through legitimate
# request/event data to drive the function, but just spoofing behavior with this
def main(request):
    if request:
        data_sources_updated = check_data_source_update(data_source_list)

        # If any data sources were actually updated, post to pubsub topic
        # for subseqeuent processing in dbt
        if data_sources_updated > 0:
            message = f'{data_sources_updated} data sources updated'

            post_message_to_topic(message,
                            os.environ.get('TOPIC_PROJ_ID'),
                            os.environ.get('TOPIC_ID'))

        # Acknowleding request
        return 'Roger that'
