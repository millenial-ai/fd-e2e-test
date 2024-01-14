from argparse import ArgumentParser
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup

def main(args):
    sagemaker_session = sagemaker.Session()

    transactions_feature_group_name = args.group_name
    transactions_feature_group = FeatureGroup(name=transactions_feature_group_name, sagemaker_session=sagemaker_session)
    
    sagemaker_session.update_feature_group(
        feature_group_name=transactions_feature_group_name,
        feature_additions=[
            {"FeatureName": feature.split(',')[0], "FeatureType":  feature.split(',')[1]}
        for feature in args.features])
    
    
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--group-name', type=str, help='Feature group name')
    parser.add_argument('--features', nargs='+')
    args = parser.parse_args()
    main(args)