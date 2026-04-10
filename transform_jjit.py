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

def transform_jjit():
    s3_client = boto3.client(
        's3', endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_key = f"{date_str}/jjit_jobs.json"
    
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        jobs = json.loads(response['Body'].read().decode('utf-8'))
        print(f"Wczytano {len(jobs)} surowych ofert (JJIT)")
    except Exception as e:
        print(f"Blad pobierania pliku {file_key}: {e}")
        return

    processed_data = []
    for job in jobs:
        title = job.get('title', 'Brak tytulu')
        company_name = job.get('companyName', 'Nieznana firma')
        o_id = job.get('slug', 'brak')
        category = assign_category(title)
        
        raw_date = job.get('publishedAt')
        date_added = raw_date[:10] if isinstance(raw_date, str) and len(raw_date) >= 10 else datetime.now().strftime('%Y-%m-%d')
        
        workplace_type = str(job.get('workplaceType', '')).lower()
        fully_remote = (workplace_type == 'remote')
        
        places_list = []
        if fully_remote: places_list.append("Remote")
        if job.get('city'): places_list.append(str(job.get('city')).strip())
            
        for loc in job.get('locations', []):
            if isinstance(loc, dict) and loc.get('city'):
                places_list.append(str(loc.get('city')).strip())
                
        unique_places = []
        for p in places_list:
            if p and p.lower() != 'remote' and p not in unique_places:
                unique_places.append(p)
                
        location = ", ".join(unique_places) if unique_places else 'Polska'
        url = f"https://justjoin.it/offers/{o_id}" if o_id != 'brak' else ''

        # --- NOWOŚĆ: Zarobki + Typ Umowy ---
        salary_min, salary_max, currency = None, None, 'PLN'
        contract_type = 'Inna'
        emp_types = job.get('employmentTypes', [])
        
        if isinstance(emp_types, list):
            for emp in emp_types:
                if emp.get('currencySource') == 'original':
                    if emp.get('from') is not None or emp.get('to') is not None:
                        salary_min = emp.get('from')
                        salary_max = emp.get('to')
                        currency = str(emp.get('currency', 'PLN')).upper()
                        
                        # Wyciągamy typ umowy z pobranej sekcji zarobków
                        c_type = str(emp.get('type', '')).lower()
                        if 'b2b' in c_type:
                            contract_type = 'B2B'
                        elif 'permanent' in c_type or 'uop' in c_type:
                            contract_type = 'UoP'
                        break

        tech_list = []
        skills = job.get('requiredSkills', [])
        if isinstance(skills, list):
            for skill in skills:
                if isinstance(skill, str): tech_list.append(skill.strip())
                elif isinstance(skill, dict) and 'name' in skill: tech_list.append(str(skill['name']).strip())
                    
        technologie_str = ", ".join(tech_list) if tech_list else ""

        processed_data.append({
            'id': o_id,
            'kategoria': category,
            'title': title,
            'company_name': str(company_name),
            'location': location,
            'remote': fully_remote,
            'contract_type': contract_type, # <--- DODANA KOLUMNA
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
    print(f"Po agregacji: {len(df_grouped)} ofert (JJIT).")
    
    try:
        engine = create_engine(DB_URL)
        df_grouped.to_sql('poland_job_offers', engine, if_exists='append', index=False)
        print("Zapisano do bazy (APPEND).")
    except Exception as e:
        print(f"Blad zapisu bazy: {e}")

if __name__ == "__main__":
    transform_jjit()