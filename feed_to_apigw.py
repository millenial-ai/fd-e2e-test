import requests
import csv
import argparse
from datetime import datetime

# Function to read a csv file and send it to api gateway
def read_csv(file_path, api_url, sanity):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        column_names = []
        for idx, row in enumerate(reader):
            if idx == 0:
                column_names = row
                continue
            
            if args.skip and idx <= args.skip:
                print("Skipping", idx)
                continue
            
            values = {
                column_names[i]: row[i]
                for i in range(len(row))
            }
            
            values = {
                **values,
                "tx_date": values["trans_date_trans_time"],
                "tx_name": "Anthony Nguyen",
                "tx_ending": "8899"
            }
            body = {
                'values': values
            }
            print('body', body)
            # Send the row to the API Gateway
            response = requests.post(api_url, json=body)
            print(response.text)
            
            if sanity:
                break

def main(args):
    read_csv(args.file_path, args.api_url, args.sanity)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send CSV data to API Gateway')
    parser.add_argument('--file_path', help='Path to the CSV file', default='./datacamp-creditcardfraud-100.csv')
    parser.add_argument('--api_url', help='API Gateway URL', default='https://0m99smdcz1.execute-api.us-east-1.amazonaws.com/staging/fraud/check')
    parser.add_argument('--sanity', action='store_true')
    parser.add_argument('--skip', type=int, help='Item to skip')
    args =parser.parse_args()
    
    main(args)