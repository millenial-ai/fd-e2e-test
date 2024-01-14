from sagemaker.feature_store.feature_group import FeatureGroup
import sagemaker
import pandas as pd
from sagemaker.feature_store.inputs import TableFormatEnum
import logging
import time
from datetime import datetime
from argparse import ArgumentParser

logger = logging.getLogger('__name__')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

def wait_for_feature_group_creation_complete(feature_group):
    status = feature_group.describe().get('FeatureGroupStatus')
    print(f'Initial status: {status}')
    while status == 'Creating':
        logger.info(f'Waiting for feature group: {feature_group.name} to be created ...')
        time.sleep(5)
        status = feature_group.describe().get('FeatureGroupStatus')
    if status != 'Created':
        raise SystemExit(f'Failed to create feature group {feature_group.name}: {status}')
    logger.info(f'FeatureGroup {feature_group.name} was successfully created.')

def str_to_iso_date(d: str):
    return datetime.strptime(d, '%Y-%m-%d %H:%M:%S').isoformat() + 'Z'

def main(args):
    sagemaker_session = sagemaker.Session()

    feature_group_name = args.feature_group_name
    default_bucket = "fd-featurestore-storage"
    prefix = args.feature_group_name
    role = "arn:aws:iam::348490654799:role/service-role/AmazonSageMaker-ExecutionRole-20230705T105457"
    record_identifier_name = 'trans_num'
    event_time_feature_name = 'trans_date_trans_time'
    
    transactions_feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=sagemaker_session)
    
    df = pd.read_csv(args.init_csv)
    
    # prefill 2 additional values, should be used for training
    df["rcf_isfraud"] = df["is_fraud"]
    df["xgb_isfraud"] = df["is_fraud"]
    df["xgb_score"] = -1.
    df["rcf_score"] = -1.
    df["part_of_day"] = "night"
    df["age"] = 1
    
    df[event_time_feature_name] = df[event_time_feature_name].map(lambda d: str_to_iso_date(d))
    
    transactions_feature_group.load_feature_definitions(data_frame=df)
    
    table_format = TableFormatEnum.ICEBERG
    
    create_new = True
    try:
        transactions_feature_group.describe()
        # If feature group existed
        print("Feature group existed, delete and create new? y/n")
        prompt = input()
        
        if prompt == 'y' or prompt == 'Y':
            print("Deleting existing feature group")
            transactions_feature_group.delete()
            print("Waiting for feature group to be fully deleted")
            while True:
                try:
                    transactions_feature_group.describe()
                    time.sleep(1)
                except:
                    break
        else:
            print("Doing nothing")
            create_new = False 
    except Exception as e:
        print("Feature group not existed")
    
    
    if create_new:
        print("Creating feature group")
        transactions_feature_group.create(
            s3_uri=f's3://{default_bucket}/{prefix}', 
            record_identifier_name=record_identifier_name, 
            event_time_feature_name=event_time_feature_name, 
            role_arn=role, 
            enable_online_store=False,
            table_format=table_format 
        )
          
        wait_for_feature_group_creation_complete(transactions_feature_group)
    # import IPython ; IPython.embed()
    
    # print("Updating feature group")
    # sagemaker_session.update_feature_group(
    # feature_group_name=feature_group_name,
    # feature_additions=[
    #     {"FeatureName": feature.split(',')[0], "FeatureType":  feature.split(',')[1]}
    # for feature in args.features])
    
    if not args.skip_ingestion:
        print("Ingesting initial data")
        transactions_feature_group.ingest(data_frame=df, max_processes=16, wait=True)
    else:
        print("Skipping ingestion")
    print("Done")

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--feature-group-name", type=str)
    parser.add_argument("--init-csv", type=str, default="datacamp-creditcardfraud-100.csv")
    parser.add_argument('--skip-ingestion', action='store_true')

    args = parser.parse_args()
    main(args)