import json
import sys
import requests
from zeep import Client
from google.cloud import storage
import csv

class SalesforceBulkExtractor:
    def __init__(self, wsdl_path, access_token, instance_url):
        # ローカルのWSDLファイルを使用
        self.client = Client(wsdl_path)
        self.session_header = {'SessionHeader': {'sessionId': access_token}}
        self.instance_url = instance_url

    def query_data(self, soql_query):
        service_url = f"{self.instance_url}/services/Soap/c/53.0"
        self.client.transport.session.headers.update({
            'Authorization': f'Bearer {self.session_header["SessionHeader"]["sessionId"]}',
            'Content-Type': 'text/xml'
        })
        # SOQLクエリでデータを取得
        response = self.client.service.query(self.session_header, soql_query)
        return response['records']

class GCSUploader:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
    
    def upload_csv(self, destination_blob_name, data, fields):
        blob = self.bucket.blob(destination_blob_name)
        with blob.open("w") as f:
            writer = csv.writer(f)
            writer.writerow(fields)
            for record in data:
                row = [record.get(field) for field in fields]
                writer.writerow(row)

def load_config(config_file):
    with open(config_file) as f:
        return json.load(f)

def load_soql_query(query_file):
    with open(query_file) as f:
        return f.read().strip()

def get_salesforce_token(consumer_key, consumer_secret, username, password):
    token_url = "https://login.salesforce.com/services/oauth2/token"
    payload = {
        'grant_type': 'password',
        'client_id': consumer_key,
        'client_secret': consumer_secret,
        'username': username,
        'password': password  # パスワード + セキュリティトークン
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        return response.json()['access_token'], response.json()['instance_url']
    else:
        raise Exception(f"Failed to obtain token: {response.text}")

def main():
    # 設定ファイルの読み込み
    config = load_config('config.json')
    
    # 実行時の引数からオブジェクト名を取得
    if len(sys.argv) < 2:
        print("Usage: python main.py <ObjectName>")
        sys.exit(1)
    object_name = sys.argv[1]
    
    # OAuth認証でアクセストークンとインスタンスURLを取得
    consumer_key = config['consumer_key']
    consumer_secret = config['consumer_secret']
    username = config['username']
    password = config['password']  # セキュリティトークンを含める
    access_token, instance_url = get_salesforce_token(consumer_key, consumer_secret, username, password)
    
    # WSDLファイルのローカルパスを指定
    wsdl_path = config['wsdl_path']
    
    # 外部ファイルからSOQLクエリを読み込む
    query_file = f"queries/{object_name}.soql"  # クエリファイルのパス
    soql_query = load_soql_query(query_file)
    
    # Salesforceからデータを抽出
    sf_extractor = SalesforceBulkExtractor(wsdl_path, access_token, instance_url)
    data = sf_extractor.query_data(soql_query)
    
    # データのフィールドはSOQLクエリに依存
    fields = soql_query.replace("SELECT", "").split("FROM")[0].strip().split(", ")
    
    # GCSにCSVファイルとしてアップロード
    gcs_uploader = GCSUploader(config['bucket_name'])
    gcs_uploader.upload_csv(f'salesforce_{object_name}.csv', data, fields)

if __name__ == '__main__':
    main()
