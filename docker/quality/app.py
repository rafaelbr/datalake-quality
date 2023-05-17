import datetime
import io
import os
import uuid

import great_expectations as ge

import awswrangler as wr
import pandas as pd
import boto3
import json
import logging

from great_expectations.core import ExpectationSuiteSchema, ExpectationSuite
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, AnonymizedUsageStatisticsConfig
from great_expectations.dataset import PandasDataset

CONFIG_BUCKET = os.getenv('CONFIG_BUCKET')
GE_PATH = os.getenv('GE_PATH')
CONFIG_PATH = os.getenv('CONFIG_PATH')
TARGET_BUCKET = os.getenv('TARGET_BUCKET')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Ler arquivo de configuracao
def read_config():
    s3 = boto3.resource('s3')
    obj = s3.Object(CONFIG_BUCKET, CONFIG_PATH)
    body = obj.get()['Body'].read()
    return json.loads(body)


# Ler arquivo no bucket

config = read_config()

def check_file_exists_s3(bucket, key):
    s3 = boto3.resource('s3')

    return_var = False
    try:
        s3.Object(bucket, key).load()
        return_var = True
    except Exception as e:
        print(e)
        return_var = False
    finally:
        s3.close()

    return return_var

def read_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    body = obj.get()['Body'].read()
    return body

# Validar arquivo na configuracao
def check_database_config(bucket, key):

    key_parts = key.split('/')

    if len(key_parts) < 4:
        logger.error('Invalid key')
        return None

    suggested_database = key_parts[0]
    suggested_table = key_parts[1]
    suggested_format = key_parts[2]
    suggested_extension = key_parts[3].split('.')[-1]

    print({
        'suggested_database': suggested_database,
        'suggested_table': suggested_table,
        'suggested_format': suggested_format,
        'suggested_extension': suggested_extension
    })

    if suggested_format != suggested_extension:
        logger.error('Invalid format')
        return None

    data = None
    for d in config['databases']:
        if d['name'] == suggested_database:
            key_parts = key_parts[1:]
            for t in d['tables']:
                if t['name'] == suggested_table:
                    data = {
                        'database': d['name'],
                        'table': t['name'],
                        'format': suggested_format,
                        'delimiter': t['delimiter'],
                        'append': d['append'],
                        'partitions': t['partitions']
                    }
                    break
            break
    return data

def process_s3_file(bucket, key):
    data = check_database_config(bucket, key)
    if data is None:
        logger.error('No config matched the file')
        return None
    body = read_from_s3(bucket, key)

    data_df = None
    if data['format'] == 'csv':
        data_df = pd.read_csv(io.StringIO(body.decode('utf-8')), sep=data['delimiter'])
    elif data['format'] == 'json':
        data_df = pd.read_json(io.StringIO(body.decode('utf-8')))
    elif data['format'] == 'parquet':
        data_df = wr.s3.read_parquet('s3://{}/{}'.format(bucket, key))

    return {
        "info": data,
        "df": data_df
    }

# Validar expectativas
def validate_with_great_expectations(data):
    data_context_config = DataContextConfig(
        datasources={
            "pandas_datasource": DatasourceConfig(
                class_name="PandasDatasource"
            )
        },
        stores={
            "expectations_S3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": CONFIG_BUCKET,
                    "prefix": GE_PATH
                }
            }
        },
        expectations_store_name="expectations_S3_store"
    )

    context = BaseDataContext(project_config=data_context_config)
    suite = context.get_expectation_suite('{}.{}'.format(data['info']['database'], data['info']['table']))
    batch = PandasDataset(data['df'], expectation_suite=suite)
    results = batch.validate()

    return results

def make_athena_partition_on_s3(bucket, key):
    data = check_database_config(bucket, key)
    if data is None:
        logger.error('No config matched the file')
        return None

    file_name = key.split('/')[-1]
    file_parts = file_name.split('.')

    # format of file name: {table_name}.{partition_value1}.{partition_value2}..{partition_value3}.{date}.{format}
    partition_key = f'{data["database"]}/{data["table"]}/'
    if len(data['partitions']) > 0 and len(file_parts) > 3:
        table_name = file_parts[0]
        partition_values = file_parts[1:-2]
        date = file_parts[-2]
        format = file_parts[-1]

        if len(partition_values) != len(data['partitions']):
            logger.error('Invalid partition values')
            return None

        for i in range(len(partition_values)):
            partition_key += f'{data["partitions"][i]}={partition_values[i]}/'

    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')

    partition_key += f'year={year}/month={month}/day={day}/'

    return partition_key

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    data = process_s3_file(bucket, key)

    if data is None:
        return 'Failure'

    if data['df'] is None:
        return None

    database = data['info']['database']
    table = data['info']['table']

    #data quality check
    logger.info('Running data quality check')
    results = validate_with_great_expectations(data)
    logger.info(results)

    if not results['success']:
        logger.error('Data quality check failed')
        return 'Failure'

    key_write = make_athena_partition_on_s3(bucket, key)

    logger.info('Writing to s3')
    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')
    wr.s3.to_parquet(data['df'], f's3://{TARGET_BUCKET}/{key_write}', dataset=True, mode='append')
    return "Success"

