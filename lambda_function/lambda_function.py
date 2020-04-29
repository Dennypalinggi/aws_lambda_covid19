import requests
import pandas as pd
from pandas import json_normalize
import boto3
from datetime import datetime
from io import StringIO # python3; python2: BytesIO 

#C:\Users\Usuario\lambdacode\Lib\site-packages

def lambda_handler(event, context):
    
    baseUrl = "https://api.covid19api.com/total/dayone/country/indonesia/status/confirmed"
    response = requests.get(baseUrl)
    df = json_normalize(response.json())
    df['change (%)'] = df['Cases'].pct_change()*100
    
    
    bucketname = 'my-beautiful-bucket'
    filename = 'corona_dataset.csv'
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    
    client = boto3.client('s3')
    
    response = client.put_object(
        ACL = 'private',
        Body= csv_buffer.getvalue(),
        Bucket= bucketname,
        Key= filename
    )