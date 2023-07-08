from datetime import datetime
import random
from faker import Faker
import uuid
from google.cloud import storage
import json

def create_transactions(request):
    request_json = request.get_json()
    if request_json and 'quantity' in request_json:
        quantity = request_json['quantity']
    else:
        return json.dumps({"error": "Missing 'quantity'"}), 400

    date_str = datetime.now().strftime('%Y%m%d')

    uuid_4dig = str(uuid.uuid4().hex)[:4]

    bucket_name = 'dev-ronny-datalake-raw'
    destination_blob_name = f'results/transactions_{date_str}_{uuid_4dig}.txt'

    fake = Faker()
    lines = []

    for _ in range(quantity):
        transaction_id = fake.random_number(digits=6)
        income = round(random.uniform(10, 1000), 2)
        country = fake.country()
        date_time = fake.date_time_between(start_date='-3h')
        line = f'{transaction_id}\t{income}\t{country}\t{date_time}\n'
        lines.append(line)

    text_content = ''.join(lines)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(text_content, content_type='text/plain')

    message = f'Object created at {bucket_name} with name {destination_blob_name}'

    return json.dumps({"message": message})