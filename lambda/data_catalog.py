import json
import logging
import os

import boto3

CONFIG_BUCKET = os.getenv('CONFIG_BUCKET')
CONFIG_PATH = os.getenv('CONFIG_PATH')
TRUSTED_DATABASE = os.getenv('TRUSTED_DATABASE')
TRUSTED_BUCKET = os.getenv('TRUSTED_BUCKET')
AWS_RESOURCES_BUCKET = os.getenv('AWS_RESOURCES_BUCKET')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_config():
    s3 = boto3.resource('s3')
    obj = s3.Object(CONFIG_BUCKET, CONFIG_PATH)
    body = obj.get()['Body'].read()
    return json.loads(body)


config = read_config()

def check_database_config(bucket, key):
    key_parts = key.split('/')

    if len(key_parts) < 4:
        logger.error('Invalid key')
        return None

    suggested_database = key_parts[0]
    suggested_table = key_parts[1]

    data = None
    for d in config['databases']:
        if d['name'] == suggested_database:
            for t in d['tables']:
                if t['name'] == suggested_table:
                    data = {
                        'database': d['name'],
                        'table': t['name'],
                        'schema': t['schema'],
                        'partitions': t['partitions']
                    }
                    break
            break
    return data

# ler a estrutura desse arquivo
def check_glue_table_exists(database_config):
    glue_client = boto3.client('glue')

    table_name = f'{database_config["database"]}_{database_config["table"]}'

    return_var = False
    try:
        response = glue_client.get_table(
            DatabaseName=TRUSTED_DATABASE,
            Name=table_name
        )

        if response.get('Table'):
            logger.info(f'Table {table_name} already exists')
            return_var = True
        else:
            logger.info(f'Table {table_name} does not exist')
            return_var = False
    except glue_client.exceptions.EntityNotFoundException:
        logger.info(f'Table {table_name} does not exist')
        return_var = False
    finally:
        glue_client.close()

    return return_var

# criar ou atualizar a tabela no glue
def create_glue_table(database_config, s3_path):
    glue_client = boto3.client('glue')

    table_name = f'{database_config["database"]}_{database_config["table"]}'
    schema = database_config['schema']
    schema = [{'Name': column['name'], 'Type': column['type']} for column in schema]
    partition_columns = database_config['partitions']

    table_input = {
        'Name': table_name,
        'Description': 'Table criada pelo código Python',
        'StorageDescriptor': {
            'Columns': schema,
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            }
        },
        'PartitionKeys': partition_columns
    }

    glue_client.create_table(DatabaseName=TRUSTED_DATABASE, TableInput=table_input)
    logger.info(f'Table {table_name} created successfully with partitions {partition_columns}')
    glue_client.close()

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key_path = '/'.join(key.split('/')[:2])
    s3_path = f's3://{bucket}/{key_path}/'

    database_config = check_database_config(bucket, key)
    if database_config is None:
        logger.error('No config matched the file')
        return 'Failure'

    if not check_glue_table_exists(database_config):
        create_glue_table(database_config, s3_path)

    ##execute MSCK REPAIR TABLE on Athena
    athena_client = boto3.client('athena')
    table_name = f'{database_config["database"]}_{database_config["table"]}'
    query = f'MSCK REPAIR TABLE {table_name}'
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': TRUSTED_DATABASE
        },
        ResultConfiguration={
            'OutputLocation': f's3://{AWS_RESOURCES_BUCKET}/athena/'
        }
    )
    athena_client.close()
    return 'Success'