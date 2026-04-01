import boto3
import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from botocore.client import Config

S3_ENDPOINT = 'http://127.0.0.1:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'supersecretpassword'
BUCKET_NAME = 'raw-data'
DB_URL = os.environ.get("DB_URL")

def assign_category(title_str):
    text = str(title_str).lower()
    if any(kw in text for kw in ['data', 'sql', 'machine learning', 'ai', 'pandas', 'analyst']): return 'Data & AI'
    if any(kw in text for kw in ['react', 'angular', 'vue', 'frontend', 'javascript']): return 'Frontend'
    if any(kw in text for kw in ['python', 'django', 'java', 'spring', 'node', 'backend', 'c#', 'php', '.net']): return 'Backend'
    if any(kw in text for kw in ['aws', 'azure', 'docker', 'devops', 'cloud']): return 'DevOps & Cloud'
    if any(kw in text for kw in ['tester', 'qa', 'test']): return 'Testing'
    return 'Inne'

def transform_poland():
    s3_client = boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_key = f"{date_str}/poland_jobs.json"
    
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        raw_data = response['Body'].read().decode('utf-8')
        jobs = json.loads(raw_data)
    except Exception as e:
        print(f"Blad pobierania pliku {file_key}: {e}")
        return

    processed_data = []
    for job in jobs:
        # 1. NAZWY FIRMY
        company_name = job.get('name') or job.get('companyName') or 'Nieznana firma'
        if isinstance(company_name, dict):
            company_name = company_name.get('name', 'Nieznana firma')
            
        # 2. TYTUL STANOWISKA I KATEGORIA
        title = job.get('title', 'Brak tytulu')
        o_id = job.get('id', 'brak')
        category = assign_category(title)
        
        # 3. DATA DODANIA
        raw_date = job.get('renewed') or job.get('posted') or job.get('timestamp')
        try:
            if raw_date:
                # Jesli to Unix Timestamp w milisekundach
                if isinstance(raw_date, (int, float)) and raw_date > 9999999999:
                    date_added = datetime.fromtimestamp(raw_date / 1000).strftime('%Y-%m-%d')
                elif isinstance(raw_date, str):
                    date_added = raw_date[:10] 
                else:
                    date_added = datetime.now().strftime('%Y-%m-%d')
            else:
                date_added = datetime.now().strftime('%Y-%m-%d')
        except Exception:
            date_added = datetime.now().strftime('%Y-%m-%d')
        
        # 4. LOKALIZACJA
        places_list = []
        fully_remote = job.get('fullyRemote', False)
        if fully_remote:
            places_list.append("Remote")
            
        location_data = job.get('location', {})
        raw_places = location_data.get('places', []) if isinstance(location_data, dict) else []
        
        if isinstance(raw_places, list):
            seen_cities = set()
            for p in raw_places:
                city = p.get('city')
                if city and city.strip() and city.lower() not in seen_cities:
                    if not fully_remote or city.lower() != "remote":
                        places_list.append(city.strip())
                        seen_cities.add(city.lower())
                        
        location = ", ".join(places_list) if places_list else 'Polska'
        
        # 5. ZAROBKI I URL
        salary_data = job.get('salary', {})
        salary_min = salary_data.get('from', None)
        salary_max = salary_data.get('to', None)
        currency = salary_data.get('currency', 'PLN')
        url = f"https://nofluffjobs.com/pl/job/{o_id}" if o_id != 'brak' else ''

        processed_data.append({
            'id': o_id,
            'kategoria': category,
            'title': title,
            'company_name': str(company_name),
            'location': location,
            'remote': fully_remote,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'currency': currency,
            'url': url,
            'date_added': date_added
        })

    # --- TWORZENIE TABELI I USUWANIE DUPLIKATOW PO ID ---
    df = pd.DataFrame(processed_data)
    l_poczatkowa = len(df)
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    
    # --- PRZENIESIENIE "REMOTE" Z LOKALIZACJI DO KOLUMNY ZDALNIE ---
    df['remote'] = df['remote'].astype(bool) | df['location'].str.contains('Remote', na=False, case=False)

    # --- Lączenie "City Spammingu" i czyszczenie ---
    def scal_lokalizacje(seria_lokalizacji):
        zbior_miast = set()
        for loc in seria_lokalizacji:
            if pd.notna(loc):
                miasta = [m.strip() for m in str(loc).split(',')]
                zbior_miast.update(miasta)
        
        lista_miast = list(zbior_miast)
        # Wycinamy "Remote" 
        lista_miast = [m for m in lista_miast if m.lower() != 'remote']
            
        if lista_miast:
            return ", ".join(sorted(lista_miast))
        else:
            return "Brak (tylko zdalnie)"

    sposob_agregacji = {
        'id': 'first',
        'kategoria': 'first',
        'location': scal_lokalizacje,
        'remote': 'max',
        'salary_min': 'first',
        'salary_max': 'first',
        'currency': 'first',
        'url': 'first',
        'date_added': 'max'
    }

    # Grupowanie po Tytule i Firmie
    df_grouped = df.groupby(['title', 'company_name'], as_index=False).agg(sposob_agregacji)
    
    l_koncowa = len(df_grouped)
    print(f"Początkowa liczba pobranych ofert (po ID): {l_poczatkowa}")
    print(f"Połączono klony z różnych miast. Zostało: {l_koncowa} unikalnych stanowisk.")
    
    # --- ZAPIS DO BAZY ---
    try:
        engine = create_engine(DB_URL)
        df_grouped.to_sql('poland_job_offers', engine, if_exists='replace', index=False)
        print("Zapisano do bazy danych PostgreSQL. Dashboard gotowy!")
    except Exception as e:
        print(f"Blad zapisu bazy danych: {e}")

if __name__ == "__main__":
    transform_poland()
