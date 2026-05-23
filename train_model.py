import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor
import joblib
import warnings

warnings.filterwarnings('ignore')

# 1. POŁĄCZENIE Z BAZĄ DANYCH
DB_URL = os.environ.get("DB_URL")
engine = create_engine(DB_URL)

print("Pobieranie danych z bazy Neon...")
df = pd.read_sql("SELECT * FROM poland_job_offers;", engine)

# 2. CZYSZCZENIE I PRZYGOTOWANIE DANYCH
print("Czyszczenie danych i standaryzacja walut...")
df = df.dropna(subset=['salary_min', 'salary_max'])

if 'currency' in df.columns:
    df['currency'] = df['currency'].fillna('PLN').str.upper()
else:
    df['currency'] = 'PLN'

exchange_rates = {'PLN': 1.0, 'USD': 4.00, 'EUR': 4.30, 'GBP': 5.00, 'CHF': 4.40}

def convert_to_pln(row, col_name):
    curr = row['currency']
    rate = exchange_rates.get(curr, 1.0)
    return row[col_name] * rate

df['salary_min_pln'] = df.apply(lambda row: convert_to_pln(row, 'salary_min'), axis=1)
df['salary_max_pln'] = df.apply(lambda row: convert_to_pln(row, 'salary_max'), axis=1)
df['target_salary'] = (df['salary_min_pln'] + df['salary_max_pln']) / 2

df = df[(df['target_salary'] >= 3000) & (df['target_salary'] <= 60000)]

def get_seniority(title):
    title = str(title).lower()
    if any(word in title for word in ['junior', 'trainee', 'intern', 'młodszy', 'staż']): return 'Junior'
    elif any(word in title for word in ['senior', 'lead', 'expert', 'principal', 'arch', 'starszy', 'head']): return 'Senior'
    else: return 'Mid'

df['seniority'] = df['title'].apply(get_seniority)
df['remote'] = df['remote'].fillna(False).astype(str)

if 'contract_type' not in df.columns:
    df['contract_type'] = 'Inna'
df['contract_type'] = df['contract_type'].fillna('Inna')

# 3. EKSTRAKCJA TECHNOLOGII
print("Analiza stacku technologicznego...")
df['technologie'] = df['technologie'].fillna('').str.lower()

# Stara gwardia
df['tech_python'] = df['technologie'].str.contains(r'python|django|flask|fastapi').astype(int)
df['tech_java'] = df['technologie'].str.contains(r'java|spring|hibernate').astype(int)
df['tech_data_sql'] = df['technologie'].str.contains(r'sql|data|bi|pandas|spark|machine learning|ai').astype(int)
df['tech_cloud'] = df['technologie'].str.contains(r'aws|cloud|azure|gcp|docker|kubernetes|terraform').astype(int)
df['tech_frontend'] = df['technologie'].str.contains(r'react|angular|vue|javascript|typescript').astype(int)

# NOWOŚĆ: Technologie dla nowych kategorii
df['tech_csharp_net'] = df['technologie'].str.contains(r'c#|\.net').astype(int)
df['tech_cpp_gamedev'] = df['technologie'].str.contains(r'c\+\+|unity|unreal|gameplay').astype(int)
df['tech_mobile'] = df['technologie'].str.contains(r'swift|kotlin|flutter|android|ios|react native').astype(int)
df['tech_php_ruby'] = df['technologie'].str.contains(r'php|laravel|symfony|ruby|rails').astype(int)
df['tech_testing_qa'] = df['technologie'].str.contains(r'selenium|cypress|qa|testing|postman|jira').astype(int)
df['tech_erp_crm'] = df['technologie'].str.contains(r'sap|salesforce|abap|dynamics').astype(int)

# 4. PRZYGOTOWANIE DO TRENINGU (Dodane nowe wymiary)
features = df[['kategoria', 'location', 'seniority', 'remote', 'contract_type', 
               'tech_python', 'tech_java', 'tech_data_sql', 'tech_cloud', 'tech_frontend',
               'tech_csharp_net', 'tech_cpp_gamedev', 'tech_mobile', 'tech_php_ruby', 
               'tech_testing_qa', 'tech_erp_crm']] 

print(f"Liczba ofert gotowych do nauki: {len(df)}")

X = pd.get_dummies(features)
y = df['target_salary']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 5. TRENOWANIE MODELU (ZMIANA SILNIKA NA XGBOOST)
print("Trenowanie algorytmu Sztucznej Inteligencji (XGBoost)...")
model = XGBRegressor(
    n_estimators=300,       # Liczba sekwencyjnych drzew
    learning_rate=0.05,     # Tempo nauki
    max_depth=7,            # Maksymalna głębokość pojedynczego drzewa
    random_state=42,
    n_jobs=-1               # Wykorzystaj wszystkie rdzenie procesora
)
model.fit(X_train, y_train)

# 6. EGZAMIN I OCENA
predictions = model.predict(X_test)
mae = mean_absolute_error(y_test, predictions)

print("-" * 50)
print(f"✅ Baza wiedzy zaktualizowana (XGBoost)!")
print(f"✅ Średni błąd modelu (MAE): {mae:.0f} PLN")
print("-" * 50)

# 7. EKSPORT MODELU
joblib.dump(model, 'salary_model.pkl')
joblib.dump(list(X.columns), 'model_columns.pkl')
print("Model i struktura kolumn zapisane pomyślnie.")
