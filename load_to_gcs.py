from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import yaml

def loadDataToGcs(projectId,cloudStorageBucket):

    CHUNK_SIZE = 128 * 1024 * 1024
    
    client_secrets = yaml.load(open("InnoMinds-d37afa5520da.json"))
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

    client = storage.Client(credentials=credentials, project = projectId)
    bucket = client.get_bucket(cloudStorageBucket)
    blob = bucket.blob('amazon_reviews_us_Shoes_v1_00.tsv.gz', CHUNK_SIZE)
    blob.upload_from_filename('amazon_reviews_us_Shoes_v1_00.tsv.gz')


def main_run(success):
    projectId = ['innominds']
    cloudStorageBucket = ['dataengineering_store']
    filesToLoad = ['amazon_reviews_us_Shoes_v1_00.tsv.gz']
    
    for i in projectId:
        for j in cloudStorageBucket:
            loadDataToGcs(i,j)

    return True