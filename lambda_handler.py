import json
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
    print(headers)
    print(query_string_params)
    bc_session = bc.get_session()
    
    session = boto3.Session(
            botocore_session=bc_session,
            region_name=region,
        )
    
    # Setup the client
    client_redshift = session.client("redshift-data")
    print("Data API client successfully loaded")
    
    query_str = "select  part_ssn, sum(employee_contribution) employee, sum(agency_automatic) automatic, sum(matching_dollar_contribution) matching, sum(row_total) row_total from {} where PART_SSN='{}'  group by part_ssn".format(table_name, dict(query_string_params)["ssn"])
                      
    print(query_str)
    
    res = client_redshift.execute_statement(Database= db_name, SecretArn= secret_arn, Sql= query_str, ClusterIdentifier= cluster_id)
    
    id=res["Id"]
    
    query_status = client_redshift.describe_statement(Id=id)["Status"]

    while query_status!='FINISHED' and query_status!='ABORTED' and query_status!='FAILED':

        query_status = client_redshift.describe_statement(Id=id)["Status"]
    
    print(query_status)
    if query_status == "FAILED":
        print("Error Detail: "+client_redshift.describe_statement(Id=id)["Error"])
    nextToken = query_string_params.get("nextToken",None)
    response = None
    
    if nextToken is None and query_status=='FINISHED':
        response = client_redshift.get_statement_result(
            Id=id
        )
    elif query_status=='FINISHED':
        response = client_redshift.get_statement_result(
            Id=id,
            NextToken=nextToken
        )
    
    result = []
    
    for row in response["Records"]:
        part_ssn = str(row[0][list(row[0].keys())[0]])
        employee =   row[1][list(row[1].keys())[0]] if list(row[1].keys())[0]!="isNull" else "0"
        automatic =  row[2][list(row[2].keys())[0]] if list(row[2].keys())[0]!="isNull" else "0"
        matching =   row[3][list(row[3].keys())[0]] if list(row[3].keys())[0]!="isNull"  else "0"
        row_tot =    row[4][list(row[4].keys())[0]] if list(row[4].keys())[0]!="isNull" else "0"
        result.append({"ssn":part_ssn,"employee":employee,"automatic":automatic,"matching":matching,"row_total":row_tot})
        
    response = {
        "isBase64Encoded":False,
        "statusCode": 200,
        "headers": {"Content-Type":"application/json"},
        "body":json.dumps({
            "row": result,
            "nextToken": response["NextToken"]
        })
    }
    
    return response
