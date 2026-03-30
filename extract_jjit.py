import requests
import boto3
import json
import time
from datetime import datetime
from botocore.client import Config

S3_ENDPOINT = 'http://127.0.0.1:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'supersecretpassword'
BUCKET_NAME = 'raw-data'

def extract_jjit():
    s3_client = boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )

    all_offers = []
    offset = 0
    limit = 100
    
    # Nagłówki maskujące - symulacja przeglądarki
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://justjoin.it/"
    }

    print("Rozpoczęcie pobierania danych z API Just Join IT...")

    while True:
        url = f"https://justjoin.it/api/candidate-api/offers?from={offset}&itemsCount={limit}&currency=pln&orderBy=descending&sortBy=publishedAt"
        
        try:
            print(f"Pobieranie paczki ofert: offset {offset}...")
            response = requests.get(url, headers=headers, timeout=15)
            
            # Weryfikacja ewentualnej blokady
            if response.status_code != 200:
                print(f"Błąd API (Status {response.status_code}). Prawdopodobna blokada antybotowa lub koniec ofert.")
                break
                
            data = response.json()
            
            # Elastyczne rozpakowanie odpowiedzi API
            if isinstance(data, dict):
                oferty_w_paczce = data.get('data') or data.get('items') or data.get('offers') or []
            else:
                oferty_w_paczce = data
            
            # Warunek zatrzymania pętli - pusty wynik
            if not oferty_w_paczce or len(oferty_w_paczce) == 0:
                print("Osiągnięto koniec dostępnych ofert JJIT.")
                break
                
            all_offers.extend(oferty_w_paczce)
            offset += limit
            
            # Bezpieczny odpoczynek między zapytaniami
            time.sleep(1)
            
        except Exception as e:
            print(f"Błąd krytyczny podczas pobierania paczki: {e}")
            break

    print(f"Zakończono. Całkowita liczba pobranych ofert JJIT: {len(all_offers)}")

    if len(all_offers) > 0:
        date_str = datetime.now().strftime("%Y-%m-%d")
        file_key = f"{date_str}/jjit_jobs.json"
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_key,
                Body=json.dumps(all_offers, ensure_ascii=False).encode('utf-8')
            )
            print(f"Poprawnie zapisano plik w jeziorze danych: {file_key}")
        except Exception as e:
            print(f"Błąd zapisu do MinIO: {e}")

if __name__ == "__main__":
    extract_jjit()