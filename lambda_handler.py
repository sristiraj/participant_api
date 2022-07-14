import json
import requests
from datetime import datetime
from urllib.error import HTTPError
import boto3
import boto3
import botocore.session as bc
import base64
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    print(f"***** event is {event} *****")
    
    redshift_secret_name=os.environ['redshift'] ## HERE add the secret name created.
    db_name=os.environ["db_name"]
    table_name=os.environ["participant_detail_table"]
    session = boto3.session.Session()
    region = session.region_name

    client = session.client(
            service_name='secretsmanager',
            region_name=region
        )
    get_secret_value_response = client.get_secret_value(
            SecretId=redshift_secret_name
        )
    secret_arn=get_secret_value_response['ARN']
    
    secret = get_secret_value_response['SecretString']
    
    secret_json = json.loads(secret)
    
    cluster_id=secret_json['dbClusterIdentifier']    
    headers = event.get("headers","unknown")
    query_string_params = event.get("queryStringParameters","unknown")
    
    bc_session = bc.get_session()
    
    session = boto3.Session(
            botocore_session=bc_session,
            region_name=region,
        )
    
    # Setup the client
    client_redshift = session.client("redshift-data")
    print("Data API client successfully loaded")
    
    query_str = "select  part_ssn, post_date, sum(employee) employee, sum(automatic) automatic, sum(matching) matching, sum(row_total) row_total from {} where PART_SSN='{}' and post_date between to_date('{}','YYYY-MM-DD') and to_date('{}','YYYY-MM-DD') group by part_ssn, post_date, activity, fund, ae, rv;".format(table_name, query_string_params["ssn"], query_string_params["start_date"], query_string_params["end_date"])
                      
    print(query_str)
    
    res = client_redshift.execute_statement(Database= db_name, SecretArn= secret_arn, Sql= query_str, ClusterIdentifier= cluster_id)
    id=res["Id"]
    
    response = {
        "isBase64Encoded":False,
        "statusCode": 200,
        "body":{
            "row":id
        }
    }
    
    return response