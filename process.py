import aiohttp
import asyncio
from flask import Flask, jsonify, request
import requests
import gzip
import shutil
import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from bitcoin import privkey_to_address
import bitcoin
from threading import Thread

import sqlite3
def download_file(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return True
    else:
        return False

def unzip_file(file_path):
    unzipped_path = file_path[:-3]  # Remove the '.gz' extension
    with gzip.open(file_path, 'rb') as gz_file:
        with open(unzipped_path, 'wb') as unzipped_file:
            shutil.copyfileobj(gz_file, unzipped_file)
    return unzipped_path

def tsv_to_dataframe(file_path, chunk_size=10000):
    chunks = pd.read_csv(file_path, delimiter='\t', chunksize=chunk_size)
    return chunks

def check_keys_in_db(public_keys, db_file, chunk_size=10000):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Create an empty DataFrame to store the matching keys
    df_match = pd.DataFrame(columns=['address'])

    # Process the keys in chunks
    for i in range(0, len(public_keys), chunk_size):
        # Get the chunk of public keys
        chunk = public_keys[i:i+chunk_size]

        # Generate the IN clause for the SQL query
        placeholders = ', '.join(['?'] * len(chunk))

        # Execute the SQL query to check if keys are present in the database
        query = f"SELECT address FROM address WHERE address IN ({placeholders})"
        params = tuple(chunk)
        df_existing_public_keys = pd.read_sql_query(query, conn, params=params)

        # Append the matching keys to the result DataFrame
        df_match = pd.concat([df_match, df_existing_public_keys], ignore_index=True)

    # Close the database connection
    conn.close()

    return df_match

def insert_data_into_mongodb(private_keys, public_keys):
    # MongoDB connection details
    mongo_uri = "mongodb://ajar:Raja1802@ac-axrjv2u-shard-00-00.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-01.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-02.hqyaxl3.mongodb.net:27017/?ssl=true&replicaSet=atlas-5aq7oc-shard-0&authSource=admin&retryWrites=true&w=majority"
    database_name = "block"
    collection_name = "keys"

    # Create a MongoClient
    client = MongoClient(mongo_uri)

    # Access the database
    db = client[database_name]

    # Access the collection
    collection = db[collection_name]

    # Prepare the documents to insert
    documents = []
    for private_key, public_key in zip(private_keys, public_keys):
        document = {
            "private_key": private_key,
            "public_key": public_key
        }
        documents.append(document)

    # Check if documents list is not empty
    if documents:
        # Insert the documents into MongoDB
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
    else:
        print("No documents to insert.")

    client.close()



def get_cid_by_date(date_str):
    # MongoDB connection details
    mongo_uri = "mongodb://ajar:Raja1802@ac-axrjv2u-shard-00-00.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-01.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-02.hqyaxl3.mongodb.net:27017/?ssl=true&replicaSet=atlas-5aq7oc-shard-0&authSource=admin&retryWrites=true&w=majority"
    database_name = "block"
    collection_name = "tnx_ipfs"

    # Create a MongoClient
    client = MongoClient(mongo_uri)

    # Access the database
    db = client[database_name]

    # Access the collection
    collection = db[collection_name]

    # Find the document based on date_str
    query = {"date_str": date_str}
    result = collection.find_one(query)

    # Retrieve the cid from the result
    if result:
        cid = result.get("cid")
        client.close()
        return cid
    else:
        client.close()
        return None



app = Flask(__name__)

def process_data_async(date_str):
    # Check if the database has a document for the current date
    cid_result = get_cid_by_date(date_str)

    if cid_result:
        print(f"CID for date {date_str}: {cid_result}")

        # Download the file from IPFS
        # https://bafybeih6wfu7o3w6yyapwu7cfcsczvb432iahw2f3o24jmv7s3fs2n3fda.ipfs.nftstorage.link/ipfs/bafybeih6wfu7o3w6yyapwu7cfcsczvb432iahw2f3o24jmv7s3fs2n3fda/blockchair_bitcoin_transactions_20150101.tsv.gz
        url = f"https://{cid_result}.ipfs.nftstorage.link/ipfs/{cid_result}/blockchair_bitcoin_transactions_{date_str}.tsv.gz"
        file_path = f"data/blockchair_bitcoin_transactions_{date_str}.tsv.gz"
        if download_file(url, file_path):
            print(f"File downloaded for date {date_str}")

            # Unzip the file
            unzipped_path = unzip_file(file_path)
            print(f"Unzipped file path: {unzipped_path}")

            # Process the DataFrame in chunks
            chunk_iterator = tsv_to_dataframe(unzipped_path)
            for chunk in chunk_iterator:
                # Perform operations on each chunk
                private_keys = chunk['hash']

                # Convert private keys to public keys using vectorized operations
                public_keys = np.vectorize(privkey_to_address)(private_keys)

                # Convert the public keys to a Pandas Series
                public_keys_series = pd.Series(public_keys)

                # Check if the public keys are present in the database
                keys_in_db = check_keys_in_db(public_keys_series, 'address.db')
                print(keys_in_db)
                # Filter the private keys and public keys based on keys present in the database
                private_keys_filtered = private_keys[keys_in_db.index]
                public_keys_filtered = public_keys_series[keys_in_db.index]

                # Insert the data into MongoDB
                insert_data_into_mongodb(private_keys_filtered, public_keys_filtered)

            # Delete the file after processing the chunks
            os.remove(unzipped_path)
            print(f"File deleted: {unzipped_path}")

        else:
            print(f"Failed to download file for date {date_str}")
        os.remove(file_path)
        print(f"File deleted: {file_path}")
    else:
        print(f"No document found for the date {date_str}.")

def process_data_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        try:
            process_data_async(date_str)
        except:
            print(f"failed on {date_str}")
        current_date += timedelta(days=1)

@app.route('/process', methods=['POST'])
def process_dates():
    start_date_str = request.json['start_date']
    end_date_str = request.json['end_date']

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Start the data processing in a separate thread
    thread = Thread(target=process_data_range, args=(start_date, end_date))
    thread.start()

    # Return a response to the client immediately
    return jsonify({'status': 'Data processing started in the background.'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6000)