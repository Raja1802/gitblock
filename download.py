from flask import Flask, request, jsonify
import requests
import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from threading import Thread
app = Flask(__name__)


date_format = "%Y%m%d"  # Format for the date in the URL
destination_directory = "storage_1"  # Destination directory to save the files

nft_storage_api_url = "https://api.nft.storage/upload"
#api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweDczMzhiZWI2MjYxMEJBZDJCNERBMDVBNDQ3NTIwNjRiNDM4QThjRjIiLCJpc3MiOiJuZnQtc3RvcmFnZSIsImlhdCI6MTY4NzY2NTgzMzA2MywibmFtZSI6ImJsb2NrY2hhaSJ9.FpvpeJbLLLN4WITbK1K-t3E48_QVkjJvSrArESlWxP0"  # Replace with your NFT.Storage API key
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweGJhNWQzZTU2OTlDRTllZDYwMUZhRkUwYzhiZmI1MzJCYjRFYWI5OTgiLCJpc3MiOiJuZnQtc3RvcmFnZSIsImlhdCI6MTY2NTQyNTUzNDk0OCwibmFtZSI6InNkc2QifQ.3wabmCtAPSt4_6vNdD0NCLMeZIHvMthxfs9gETb5mq4"
# Configure Tor proxy settings
proxy_host = '127.0.0.1'
proxy_port = 9050
proxies = {
    'http': f'socks5h://{proxy_host}:{proxy_port}',
    'https': f'socks5h://{proxy_host}:{proxy_port}'
}

mongo_uri = "mongodb://ajar:Raja1802@ac-axrjv2u-shard-00-00.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-01.hqyaxl3.mongodb.net:27017,ac-axrjv2u-shard-00-02.hqyaxl3.mongodb.net:27017/?ssl=true&replicaSet=atlas-5aq7oc-shard-0&authSource=admin&retryWrites=true&w=majority"  # MongoDB connection URI
database_name = "block"  # Name of the database
collection_name = "tnx_ipfs"  # Name of the collection

def download_file(url, destination_path):
    # Send a GET request to the URL using Tor proxy to download the file
    response = requests.get(url, stream=True, proxies=proxies)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the destination file in binary mode
        with open(destination_path, 'wb') as f:
            # Write the contents of the response to the file
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"File downloaded and saved successfully.")
    else:
        print(f"Error downloading the file. Status code:", response.status_code)

def upload_file(file_path):
    # Create the headers with the API key for authentication
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    # Prepare the multipart/form-data payload
    files = {
        "file": (os.path.basename(file_path), open(file_path, "rb")),
    }

    # Send the POST request to upload the file to NFT.Storage
    response = requests.post(nft_storage_api_url, headers=headers, files=files)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        upload_result = response.json()
        cid = upload_result["value"]["cid"]
        print(f"File uploaded successfully. CID: {cid}")
        return cid
    else:
        print(f"Error uploading the file. Status code:", response.status_code)
        return None

def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"Local file deleted successfully.")
    except OSError:
        print(f"Error deleting the local file.")

def upload_cid_to_mongo(cid, date_str, file_name):
    # Connect to MongoDB
    client = MongoClient(mongo_uri)

    # Select the database and collection
    db = client[database_name]
    collection = db[collection_name]

    # Create the document to insert
    document = {
        "cid": cid,
        "date_str": date_str,
        "file_name": file_name,
        "timestamp": datetime.now()
    }

    # Insert the document into the collection
    result = collection.insert_one(document)

    # Print the inserted document's ID
    print(f"CID uploaded to MongoDB. Document ID: {result.inserted_id}")

    # Disconnect from MongoDB
    client.close()

def process_date_range(start_date, end_date):
    # Iterate over the date range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        url = f"https://gz.blockchair.com/bitcoin/transactions/blockchair_bitcoin_transactions_{date_str}.tsv.gz"
        destination_path = os.path.join(destination_directory, f"blockchair_bitcoin_transactions_{date_str}.tsv.gz")
        try:
        
            # Download the file
            download_file(url, destination_path)

            # Upload the file and get the CID
            cid = upload_file(destination_path)

            if cid:
                # Upload the CID to MongoDB
                upload_cid_to_mongo(cid, date_str, os.path.basename(destination_path))

            # Delete the local file
            delete_file(destination_path)
        except:
            print("failed")

        # Move to the next date
        current_date += timedelta(days=1)

# Execute the processing
@app.route('/process_dates', methods=['POST'])
def process_dates():
    start_date_str = request.json['start_date']
    end_date_str = request.json['end_date']

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Start the data processing in a separate thread
    thread = Thread(target=process_date_range, args=(start_date, end_date))
    thread.start()

    # Return a response to the client immediately
    return jsonify({'status': 'Data processing started in the background.'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)
