# -*- coding: utf-8 -*-

# Imports the Google Cloud client library
from google.cloud import language
from google.cloud import bigquery
from google.cloud.language import enums
from google.cloud.language import types
from google.api_core.exceptions import InvalidArgument
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

#import other libraries
import yaml
import warnings
import numpy as np
import os
import pandas as pd
import pickle
import io
import json
from google.protobuf.json_format import MessageToDict
from google.protobuf import json_format

warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./InnoMinds-61c33bbae2a4.json"

def query_reviews(project_id,cred):
  """ 
      DESCRIPTION  :  collects the data from FB using BIG QUERY 
      INPUT        :  project id, credentials for FB, date of query
      OUTPUT       :  dictionary - fb query result
  """
  try:
    bigquery_service = build('bigquery', 'v2', credentials=cred,cache_discovery=False)
    query_to_call = """
     SELECT CONCAT(product_title,' ',review_headline ) AS text,case when 
     product_title like '%Nike%' or product_title like '%nike%' then 'nike'
     when product_title like '%Adidas%' or product_title like '%adidas%' then 'adidas'
     else 'other' end as brand_name, product_id,review_date,marketplace
     FROM `innominds.dev.product_reviews` WHERE ( product_title like '%Nike%' or product_title like '%nike%' ) 
     or 
        
     (product_title like '%Adidas%' or product_title like '%adidas%') limit 100"""
    query_request = bigquery_service.jobs()
    query_data = {
      'query': (query_to_call),
      "useLegacySql": 'false'
    }
    query_response = query_request.query(projectId=project_id,body=query_data).execute()
    print(query_response)

    return query_response
  except (Exception) as error:
    raise ValueError("query_reviews, {}".format(error))
# Makes an entity sentiment analysis call to Natural Language API 
def analyse_sentiment(product_id, text, review_date,marketplace,brand_name,nl_api_client):

    
    text = text.str.encode(encoding='utf-8', errors='strict').to_csv(index=False)
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    # Detect the entities in the text
    try:
        entities = nl_api_client.analyze_entity_sentiment(document=document, encoding_type='UTF8').entities
        return entities
    except InvalidArgument as err:
        print(err)
        return False

def convert_data_to_df(df):
  """
      DESCRIPTION : converts the bigquery query results dictionary into dataframe
      INPUT       : dictionary
      OUTPUT      : dataframe
  """
  try:
    text = []
    brand_name = []
    product_id = []
    review_date = []
    marketplace = []
    for row in df['rows']:
        text.append(row['f'][0]['v'])
        brand_name.append(row['f'][1]['v'])
        product_id.append(row['f'][2]['v'])
        review_date.append(row['f'][3]['v'])
        marketplace.append(row['f'][4]['v'])
    dict = {
                'text' : text,
                'brand_name' : brand_name,
                'product_id' : product_id,
                'review_date' : review_date,
                'marketplace' : marketplace
                
                }
    df = pd.DataFrame(dict)
    return df
  except (Exception) as error:
    raise ValueError("convert_data_to_df, {}".format(error))

def main_run(success):
    projectid= 'innominds'

    credentials = GoogleCredentials.get_application_default()

    # Clients for BigQuery and NL API.
    nl_api_client = language.LanguageServiceClient()
    bigquery_client = bigquery.Client()

   
    query_results = query_reviews(projectid,credentials)
    df = convert_data_to_df(query_results)

    analyse_sentiment(df['product_id'],df['text'],df['review_date'],df['marketplace'],df['brand_name'],nl_api_client)
    return success