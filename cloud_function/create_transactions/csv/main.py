import csv
from datetime import datetime
import random
from faker import Faker
import uuid
from google.cloud import storage
from io import StringIO
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
    destination_blob_name = f'results/transactions_{date_str}_{uuid_4dig}.csv'

    fake = Faker()
    rows = []

    for _ in range(quantity):
        transaction_id = fake.random_number(digits=10)
        income = round(random.uniform(10, 1000), 2)
        country = 'Peru'
        date_time = fake.date_time_between(start_date='-3h')
        rows.append([transaction_id, income, country, date_time.strftime('%Y-%m-%d %H:%M:%S')])

    csv_stringio = StringIO()

    writer = csv.writer(csv_stringio)
    writer.writerow(['transaction_id', 'income', 'country', 'date_time'])
    writer.writerows(rows)

    storage_client = storage.Client()    
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(csv_stringio.getvalue(), content_type='text/csv')

    message = f'Object created at {bucket_name} with name "{destination_blob_name}".'

    return json.dumps({"message": message})