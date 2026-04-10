import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor  # <--- Zmiana silnika na XGBoost
import joblib
import warnings

warnings.filterwarnings('ignore')

# 1. POŁĄCZENIE Z BAZĄ DANYCH
DB_URL = os.environ.get("DB_URL", "postgresql://neondb_owner:npg_5f4xOMGwuaey@ep-snowy-poetry-alh2m3ka-pooler.c-3.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
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
df['tech_python'] = df['technologie'].str.contains('python').astype(int)
df['tech_java'] = df['technologie'].str.contains('java|spring').astype(int)
df['tech_data_sql'] = df['technologie'].str.contains('sql|data|bi|pandas|spark|machine learning').astype(int)
df['tech_cloud'] = df['technologie'].str.contains('aws|cloud|azure|gcp|docker|kubernetes').astype(int)
df['tech_frontend'] = df['technologie'].str.contains('react|angular|vue|javascript|typescript').astype(int)

# 4. PRZYGOTOWANIE DO TRENINGU
features = df[['kategoria', 'location', 'seniority', 'remote', 'contract_type', 
               'tech_python', 'tech_java', 'tech_data_sql', 'tech_cloud', 'tech_frontend']] 

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