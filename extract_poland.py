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

def extract_nofluffjobs():
    s3_client = boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )

    all_offers = []
    page = 1
    
    # Nowy, poprawny adres API wyciągnięty z Twojego śledztwa
    url = "https://nofluffjobs.com/api/joboffers/main"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://nofluffjobs.com/pl/"
    }

    print("Rozpoczecie pobierania danych z API No Fluff Jobs...")

    while True:
        # Parametry doklejane do URL (zamiast Payloadu)
        params = {
            "pageTo": page,
            "pageSize": 100, # Prosimy o 100 ofert na strone, by bylo szybciej
            "withSalaryMatch": "true",
            "salaryCurrency": "PLN",
            "salaryPeriod": "month",
            "region": "pl",
            "language": "pl-PL"
        }
        
        try:
            print(f"Pobieranie strony {page}...")
            # Uzywamy zaktualizowanej metody GET
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"Blad API (Status {response.status_code}). Zatrzymuje pobieranie.")
                break
                
            data = response.json()
            
            # W nowym API oferty najprawdopodobniej są w kluczu 'items' lub 'postings'
            if isinstance(data, dict):
                oferty_w_paczce = data.get('items') or data.get('postings') or data.get('offers') or []
            else:
                oferty_w_paczce = data
            
            if not oferty_w_paczce or len(oferty_w_paczce) == 0:
                print("Osiagnieto koniec dostepnych ofert NFJ.")
                break
                
            all_offers.extend(oferty_w_paczce)
            page += 1
            
            time.sleep(1)
            
        except Exception as e:
            print(f"Blad krytyczny podczas pobierania strony {page}: {e}")
            break

    print(f"Zakonczono. Calkowita liczba pobranych ofert NFJ: {len(all_offers)}")

    if len(all_offers) > 0:
        date_str = datetime.now().strftime("%Y-%m-%d")
        file_key = f"{date_str}/poland_jobs.json"
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_key,
                Body=json.dumps(all_offers, ensure_ascii=False).encode('utf-8')
            )
            print(f"Poprawnie zapisano plik w jeziorze danych: {file_key}")
        except Exception as e:
            print(f"Blad zapisu do MinIO: {e}")

if __name__ == "__main__":
    extract_nofluffjobs()