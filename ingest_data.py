from sagemaker.feature_store.feature_group import FeatureGroup
import sagemaker
from sagemaker.feature_store.inputs import FeatureValue

def record_transaction_to_feature_store(feature_group_name, transaction_data):
    sagemaker_session = sagemaker.Session()

    record_to_ingest = [
        FeatureValue(key, transaction_data[key])
        for key in transaction_data
    ]
    
    transactions_feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=sagemaker_session)
    
    try:
        transactions_feature_group.put_record(record_to_ingest)
        print("Record put into Feature Store")
    except Exception as e:
        print(e)
        print("Fail to put into Feature Store")
        

if __name__ == '__main__':
    transaction_data = {
        "trans_num": "1",
        "trans_date_trans_time": "2023-11-05T09:59:06Z",
        "merchant": "merchant",
        "category": "category",
        "amt": "111",
        "city": "city",
        "state": "state",
        "lat": "100.23",
        "long": "20.42",
        "city_pop": "3000",
        "job": "job",
        "dob": "11-11-1948",
        "merch_lat": "10.4",
        "merch_long": "123.42",
        "is_fraud": "1"
    }
    record_transaction_to_feature_store(feature_group_name='transactions-staging', transaction_data=transaction_data)