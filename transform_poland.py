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
        jobs = json.loads(response['Body'].read().decode('utf-8'))
        print(f"Wczytano {len(jobs)} surowych ofert (NFJ)")
    except Exception as e:
        print(f"Blad pobierania pliku {file_key}: {e}")
        return

    processed_data = []
    for job in jobs:
        company_name = job.get('name') or job.get('companyName') or 'Nieznana firma'
        if isinstance(company_name, dict):
            company_name = company_name.get('name', 'Nieznana firma')
            
        title = job.get('title', 'Brak tytulu')
        o_id = job.get('id', 'brak')
        category = assign_category(title)
        
        raw_date = job.get('renewed') or job.get('posted') or job.get('timestamp')
        try:
            if raw_date and isinstance(raw_date, (int, float)) and raw_date > 9999999999:
                date_added = datetime.fromtimestamp(raw_date / 1000).strftime('%Y-%m-%d')
            elif raw_date and isinstance(raw_date, str):
                date_added = raw_date[:10] 
            else:
                date_added = datetime.now().strftime('%Y-%m-%d')
        except:
            date_added = datetime.now().strftime('%Y-%m-%d')
        
        places_list = []
        fully_remote = job.get('fullyRemote', False)
        if fully_remote: places_list.append("Remote")
            
        raw_places = job.get('location', {}).get('places', []) if isinstance(job.get('location'), dict) else []
        if isinstance(raw_places, list):
            seen_cities = set()
            for p in raw_places:
                city = p.get('city')
                if city and city.strip() and city.lower() not in seen_cities:
                    if not fully_remote or city.lower() != "remote":
                        places_list.append(city.strip())
                        seen_cities.add(city.lower())
                        
        location = ", ".join(places_list) if places_list else 'Polska'
        
        salary_data = job.get('salary', {})
        salary_min = salary_data.get('from', None)
        salary_max = salary_data.get('to', None)
        currency = salary_data.get('currency', 'PLN')
        url = f"https://nofluffjobs.com/pl/job/{o_id}" if o_id != 'brak' else ''

        # --- NOWOŚĆ: Typ Umowy ---
        contract_raw = str(salary_data.get('type', '')).lower()
        if 'b2b' in contract_raw:
            contract_type = 'B2B'
        elif 'permanent' in contract_raw or 'uop' in contract_raw or 'employment' in contract_raw:
            contract_type = 'UoP'
        else:
            contract_type = 'Inna'

        tech_list = []
        main_tech = job.get('technology')
        if main_tech and isinstance(main_tech, str):
            tech_list.append(main_tech.strip())
        technologie_str = ", ".join(tech_list) if tech_list else ""

        processed_data.append({
            'id': o_id,
            'kategoria': category,
            'title': title,
            'company_name': str(company_name),
            'location': location,
            'remote': fully_remote,
            'contract_type': contract_type,  # <--- DODANA KOLUMNA
            'salary_min': salary_min,
            'salary_max': salary_max,
            'currency': currency,
            'url': url,
            'date_added': date_added,
            'technologie': technologie_str
        })

    df = pd.DataFrame(processed_data)
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    df['remote'] = df['remote'].astype(bool) | df['location'].str.contains('Remote', na=False, case=False)

    def scal_lokalizacje(seria):
        zbior = set()
        for loc in seria:
            if pd.notna(loc): zbior.update([m.strip() for m in str(loc).split(',')])
        lista = [m for m in list(zbior) if m.lower() != 'remote']
        return ", ".join(sorted(lista)) if lista else "Brak (tylko zdalnie)"

    sposob_agregacji = {
        'id': 'first',
        'kategoria': 'first',
        'location': scal_lokalizacje,
        'remote': 'max',
        'contract_type': 'first', # <--- ZACHOWANIE KOLUMNY PRZY GRUPOWANIU
        'salary_min': 'first',
        'salary_max': 'first',
        'currency': 'first',
        'url': 'first',
        'date_added': 'max',
        'technologie': 'first'
    }

    df_grouped = df.groupby(['title', 'company_name'], as_index=False).agg(sposob_agregacji)
    print(f"Po agregacji: {len(df_grouped)} ofert (NFJ).")
    
    try:
        engine = create_engine(DB_URL)
        df_grouped.to_sql('poland_job_offers', engine, if_exists='replace', index=False)
        print("Zapisano do bazy (REPLACE).")
    except Exception as e:
        print(f"Blad zapisu: {e}")

if __name__ == "__main__":
    transform_poland()