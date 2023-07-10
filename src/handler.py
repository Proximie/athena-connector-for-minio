import os
import json 
from minio import Minio 
import boto3 
from pyarrow import  fs
from athena.federation.lambda_handler import AthenaLambdaHandler
from minio_data_source import MinioDataSource



def f_get_secret(secret_name,region_name):  
    """
    Function that retrieves the value of a secret stored in AWS Secrets Manager.
    Inputs:
        - secret_name: name of the secret
        - region_name: the AWS region where the secret is stored (e.g. eu-cental-1, us-east-1)
    Output:
        - secret value
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    ) 
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    ) 
    return json.loads(get_secret_value_response['SecretString'])
 
     
def f_get_minio_client(minio_credentials,scheme):
    """
    Function that connects to Minio and returns Minio client
    Inputs:
        - region_secret_name: name of the AWS secret holding Minio credentials
        - region_name: the AWS region where the secret is stored (e.g. eu-cental-1, us-east-1)
        - scheme: Minio endpoint scheme ; mainly http or https
    Outputs:
        - minio client : normal Minio client required to connect to Minio and retrieve needed data
        - minio pyarrow client: client required for pyarrow to treat Minio as an S3 file system 
    """
    region_keys = minio_credentials
    port_number = '80'
    if scheme == 'https': 
        port_number = '443'
        
    minio_client =  Minio(
                        endpoint= region_keys['server'],
                        access_key = region_keys['access_key'],
                        secret_key = region_keys['secret_key'],
                        secure = True
                        )
    
    minio_pya_client = fs.S3FileSystem(
     endpoint_override=region_keys['server'] + ':' + port_number,
     access_key=region_keys['access_key'],
     secret_key=region_keys['secret_key'],
     scheme=scheme)
    return minio_client ,minio_pya_client


# This needs to be a valid bucket that the Lambda function role has access to
spill_bucket = os.environ['TARGET_BUCKET']

# Initiate Minio Client and Minio PyArrow client
minio_credentials =  f_get_secret(os.environ['AWS_SECRET'],os.environ['AWS_REGION']) 
minio_client, minio_pya_client = f_get_minio_client(minio_credentials,os.environ['SCHEME']) 

# Get the list of buckets for the given Minio Connection
buckets = minio_client.list_buckets()

example_handler = AthenaLambdaHandler(
    data_source=MinioDataSource(minio_client, 
                                minio_pya_client,
                                buckets,
                                os.environ['TABLES_PATH'],
                                os.environ['BUCKET_PREFIX']
                                ),
    spill_bucket=spill_bucket
)

def lambda_handler(event, context):
    # For debugging purposes, we print both the event and the response :)
    print(json.dumps(event))
    response = example_handler.process_event(event)
    print(json.dumps(response))

    return response
