# -*- coding: utf-8 -*-

from google.cloud import bigquery
from gcloud import storage
import logging
import warnings
import yaml
from oauth2client.service_account import ServiceAccountCredentials
warnings.filterwarnings("ignore")

client_secrets = yaml.load(open("InnoMinds-d37afa5520da.json"))

projectId = ['innominds']
types = client_secrets['type']
client_id = client_secrets['client_id']
client_email = client_secrets['client_email']
private_key_id = client_secrets['private_key_id']
private_key = client_secrets['private_key']

credentials_dict = {
        'type': types ,
        'client_id':client_id,
        'client_email': client_email,
        'private_key_id': private_key_id,
        'private_key': private_key,
}

credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict)


logging.basicConfig(filename = 'log.txt', level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')
client_secrets = yaml.load(open("InnoMinds-d37afa5520da.json"))


def bq_create_dataset(datasetName,projectId):
    bigquery_client = bigquery.Client(credentials=credentials, project = projectId)
    dataset_ref = bigquery_client.dataset(datasetName)

    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

    return True


def bq_create_table(datasetName,tableName,bqTableSchema):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(datasetName)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(tableName)

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=bqTableSchema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

    return True

def load_data_to_gcs(projectId,cloudStorageBucket,client_secrets,credentials,filesToLoad):

    CHUNK_SIZE = 100 * 1024 * 1024

    client = storage.Client(project = projectId,credentials=credentials)
    bucket = client.get_bucket(cloudStorageBucket)

    for i in filesToLoad:
        blob = bucket.blob(i, CHUNK_SIZE)
        blob.upload_from_filename(i)
        print('File Uploaded')
    return True

def bq_load_tsv_in_gcs(bqTableSchema,cloudStoragePath,dataSetName,tableName,credentials):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataSetName)

    job_config = bigquery.LoadJobConfig()
    schema = bqTableSchema
    job_config.schema = schema
    job_config.skip_leading_rows = 1
    job_config.allow_quoted_newlines = True
    job_config.max_bad_records = 10000
    for i in cloudStoragePath:
        load_job = bigquery_client.load_table_from_uri(
            cloudStoragePath,
            dataset_ref.table(tableName),
            job_config=job_config)

    assert load_job.job_type == 'load'

    load_job.result()  # Waits for table load to complete.

    assert load_job.state == 'DONE'

    return load_job.state


def exist_record(projectId,bqTableSchema,datasetName,tableName):
    bigquery_client = bigquery.Client(credentials=credentials, project = projectId)

    query = ('SELECT id FROM `{}.{}.{}` LIMIT 1'
            .format(projectId,datasetName,tableName))

    try:
        query_job = bigquery_client.query(query)
        is_exist = len(list(query_job.result())) >= 1
        print('Exist data: {}'.format(tableName) if is_exist else 'Not exist data: {}'.format(tableName))
        return is_exist
    except Exception as e:
        print("Error")
        print(e)

    return False

