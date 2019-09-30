# -*- coding: utf-8 -*-

import bigquery_utils as bq
from gcloud import storage
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
import yaml
import logging
client_secrets = yaml.load(open("InnoMinds-d37afa5520da.json"))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./InnoMinds-61c33bbae2a4.json"

def createDataSet(dataSetName,projectId,credentials):
    bq.bq_create_dataset(dataSetName,projectId,credentials)
    return None

def loadToGcs(projectId,cloudStorageBucket,credentials,filesToLoad):
    bq.load_data_to_gcs(projectId,cloudStorageBucket,credentials,filesToLoad)

    return None

def loadDataToBigQuery(cloudStoragePath,datasetName,tableName,credentials):
    bq.bq_load_tsv_in_gcs(cloudStoragePath,datasetName,tableName,credentials)

    return None


def main_run(success):
    try:

        logging.debug("******* Loading data from S3 tsv file to Google BigQuery *******")
        projectId = ['innominds']
        cloudStorageBucket = ['dataengineering_store']
        filesToLoad = ['amazon_reviews_us_Shoes_v1_00.tsv.gz','amazon_reviews_multilingual_UK_v1_00.tsv.gz','amazon_reviews_multilingual_JP_v1_00.tsv.gz']
        fileName = ['amazon_reviews_us_Shoes_v1_00.tsv.gz']
        dataSetName = ['dev']
        tableName = ['product_reviews']
        bqTableSchema = [
        bigquery.SchemaField('marketplace', 'STRING', mode='NULLABLE', description='Age'),
        bigquery.SchemaField('custtomer_id', 'INTEGER', mode='NULLABLE', description='Name'),
        bigquery.SchemaField('review_id', 'STRING', mode='NULLABLE', description='Date and time when the record was created'),
        bigquery.SchemaField('product_id', 'STRING', mode='NULLABLE', description='product_id of the product'),
        bigquery.SchemaField('product_parent', 'INTEGER', mode='NULLABLE', description='product parent of the actual product'),
        bigquery.SchemaField('product_title', 'STRING', mode='NULLABLE', description='product_title of the product'), 
        bigquery.SchemaField('product_category', 'STRING', mode='NULLABLE', description='product category of the product'),   
        bigquery.SchemaField('star_rating', 'INTEGER', mode='NULLABLE', description='star rating of the product'),
        bigquery.SchemaField('helpful_votes', 'INTEGER', mode='NULLABLE', description='voted which are helpful to determine'),
        bigquery.SchemaField('total_votes', 'INTEGER', mode='NULLABLE', description='total votes for the product'),
        bigquery.SchemaField('vine', 'BOOLEAN', mode='NULLABLE', description='vine'),
        bigquery.SchemaField('verified_purchase', 'BOOLEAN', mode='NULLABLE', description='verified purchase or not'),
        bigquery.SchemaField('review_headline', 'STRING', mode='NULLABLE', description='review headline'),
        bigquery.SchemaField('review_body', 'STRING', mode='NULLABLE', description='review body'),
        bigquery.SchemaField('review_date', 'DATE', mode='NULLABLE', description='review date')

        ]

        cloudStoragePath = ['gs://dataengineering_store/amazon_reviews_us_Shoes_v1_00.tsv',
        'gs://dataengineering_store/amazon_reviews_multilingual_UK_v1_00.tsv.gz',
        'gs://dataengineering_store/amazon_reviews_multilingual_JP_v1_00.tsv.gz'	
        ]
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
    
        logging.debug("Creating a Dataset in bigquery if not exists")
        if len (dataSetName) != 0:
            for i in dataSetName:
                cd = createDataSet(dataSetName,projectId,credentials)
                
        logging.debug("Uploading data to cloud storage from local drive")
        if not len(projectId)!= 0 & len(cloudStorageBucket) !=0:
            for i in projectId:
                for j in cloudStorageBucket:
                    for k in filesToLoad:
                        ld = loadToGcs(i,j,credentials,k)

        logging.debug("Load tsv data file from cloud storage to Bigquery")
        if not len(cloudStoragePath) != 0 & len(tableName) != 0:
            for i in cloudStoragePath:
                loadDataToBigQuery(cloudStoragePath,dataSetName,tableName,credentials)


        logging.debug("Job Completed")    
    except(Exception) as error:
        success = False
        if(str(type(error)).split()[1].split("'")[1] == "ValueError"):
            function_name = str(error).split(",")[0]
            err = str(error).split(",")[1]
        else:
            function_name = "main_run"
            err = str(error)
        logging.debug("Error occured in Program: bigquery_demo.py")
        logging.debug("Function Name: " + str(function_name))
        logging.debug("Error: "+ str(err))
        return success
    return(success)


