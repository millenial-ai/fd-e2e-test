import requests
import json
import csv
import argparse
import random
import string

def generate_random_payload(recipient_email="caohoangtung2001@gmail.com"):
    payload = {
        "values": {
            "identifier": "user" + ''.join(random.choice(string.digits) for _ in range(2)),
            "amt": round(random.uniform(1.0, 100.0), 2),
            "lat": round(random.uniform(0, 90), 4),
            "lng": round(random.uniform(-180, 180), 4),
            "city_pop": random.randint(1, 10000),
            "merch_lat": round(random.uniform(0, 90), 6),
            "merch_lng": round(random.uniform(-180, 180), 6),
            "merch_name": ''.join(random.choice(string.ascii_letters) for _ in range(20)),
            "tx_date": f"{random.randint(2000, 2022)}-"
                        f"{random.randint(1, 12):02d}-"
                        f"{random.randint(1, 28):02d} "
                        f"{random.randint(0, 23):02d}:"
                        f"{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
            "tx_name": ''.join(random.choice(string.ascii_letters) for _ in range(12)),
            "tx_ending": ''.join(random.choice(string.digits) for _ in range(4)),
            "merchant": ''.join(random.choice(string.ascii_letters) for _ in range(20)),
            "category": ''.join(random.choice(string.ascii_lowercase) for _ in range(10)),
            "city": ''.join(random.choice(string.ascii_letters) for _ in range(10)),
            "state": ''.join(random.choice(string.ascii_uppercase) for _ in range(2)),
            "dob": f"{random.randint(1920, 2002)}-"
                    f"{random.randint(1, 12):02d}-"
                    f"{random.randint(1, 28):02d}",
            "job": ''.join(random.choice(string.ascii_letters) for _ in range(15)),
            "recipient_email": recipient_email
        }
    }
    return payload

def get_api_url_from_stage(api_stage: str):
    if api_stage == "staging":
        return "https://0m99smdcz1.execute-api.us-east-1.amazonaws.com/staging/fraud/check"
    elif api_stage == "prod":
        return "https://0m99smdcz1.execute-api.us-east-1.amazonaws.com/fraud/check"
    elif api_stage == "dev":
        return "https://0m99smdcz1.execute-api.us-east-1.amazonaws.com/dev/fraud/check"
    else:
        return "https://0m99smdcz1.execute-api.us-east-1.amazonaws.com/staging/fraud/check"

def send_transaction_to_api(transaction_data, api_stage="staging"):
    url = get_api_url_from_stage(api_stage)
    payload = json.dumps(transaction_data)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        print("Transaction sent successfully.")
        print(response.text)
    else:
        print(f"Failed to send transaction. Status code: {response.status_code}")
        print(response.text)

def main(args):
    if args.generate_random:
        for i in range(args.num_generate):
            transaction_data = generate_random_payload(recipient_email=args.recipient_email)
            send_transaction_to_api(transaction_data, api_stage=args.api_stage)
    else:
        # Read transaction data from a CSV file
        with open(args.from_csv, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for idx, row in enumerate(csv_reader):
                if idx >= args.num_generate:
                    break
                row["recipient_email"] = args.recipient_email
                transaction_data = {"values": row}
                send_transaction_to_api(transaction_data, api_stage=args.api_stage)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate-random", action="store_true")
    parser.add_argument("--num-generate", type=int, default=100)
    parser.add_argument("--api-stage", type=str, default="dev")
    parser.add_argument("--recipient-email", type=str, default="caohoangtung2001@gmail.com")
    parser.add_argument("--from-csv", type=str, default="./datacamp-creditcardfraud-100.csv")
    args = parser.parse_args()
    main(args)
