import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import joblib

DB_URL = st.secrets["DB_URL"]

@st.cache_data(ttl=3600)
def fetch_global_data():
    engine = create_engine(DB_URL)
    return pd.read_sql("SELECT * FROM job_offers;", engine)

@st.cache_data(ttl=3600)
def fetch_poland_data():
    engine = create_engine(DB_URL)
    return pd.read_sql("SELECT * FROM poland_job_offers;", engine)

st.set_page_config(page_title="Rynek Pracy IT", layout="wide")
st.title("Analityka Rynku Pracy IT")

tab_pl, tab_global, tab_tech, tab_ai = st.tabs([
    "Rynek Polski & Zarobki", 
    "Rynek Globalny", 
    "🔥 Top Technologie", 
    "🤖 Kalkulator ML"
])

# --- Zakładka 1: Rynek Globalny ---
with tab_global:
    st.header("Oferty Globalne")
    try:
        df_global = fetch_global_data()
        df_global = df_global.reset_index(drop=True)
        
        if 'Lp.' not in df_global.columns:
            df_global.insert(0, 'Lp.', range(1, len(df_global) + 1))
            
        if 'remote' in df_global.columns:
            df_global['remote'] = df_global['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")

        column_config = {
            "Lp.": st.column_config.NumberColumn("Lp.", width=50),
            "title": st.column_config.TextColumn("Stanowisko", width="large"),
            "company_name": st.column_config.TextColumn("Firma", width="medium"),
            "location": st.column_config.TextColumn("Lokalizacja", width="medium"),
            "remote": st.column_config.TextColumn("Zdalnie", width=70),
            "url": st.column_config.LinkColumn("Aplikuj", display_text="Otworz", width=70)
        }

        display_columns = ['Lp.', 'title', 'company_name', 'location', 'remote', 'url']
        existing_cols = [col for col in display_columns if col in df_global.columns]

        st.metric("Liczba ofert", len(df_global))
        st.dataframe(
            df_global[existing_cols], 
            column_config=column_config, 
            hide_index=True, 
            use_container_width=True
        )
    except Exception as e:
        st.error(f"Błąd ładowania danych globalnych: {e}")

# --- Zakładka 2: Rynek Polski ---
with tab_pl:
    st.header("Zarobki i Analiza (Polska)")
    try:
        df_pl = fetch_poland_data()
        
        if not df_pl.empty:
            df_pl['salary_avg'] = (df_pl['salary_min'] + df_pl['salary_max']) / 2
            df_pl = df_pl.reset_index(drop=True)
            
            if 'remote' in df_pl.columns:
                df_pl['remote'] = df_pl['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
                
            if 'location' in df_pl.columns:
                df_pl['location'] = df_pl['location'].astype(str).str.lstrip(', ').str.replace(r',\s*(,)+', ',', regex=True)

            st.metric("Liczba dostepnych ofert (PL)", len(df_pl))
            
            column_config_pl = {
                "date_added": st.column_config.DateColumn("Data", format="YYYY-MM-DD", width="small"),
                "kategoria": st.column_config.TextColumn("Kategoria", width="small"),
                "title": st.column_config.TextColumn("Stanowisko", width="large"),
                "company_name": st.column_config.TextColumn("Firma", width="medium"),
                "location": st.column_config.TextColumn("Lokalizacja", width="medium"),
                "remote": st.column_config.TextColumn("Zdalnie", width=70),
                "contract_type": st.column_config.TextColumn("Umowa", width="small"),  # <--- DODANA KOLUMNA
                "salary_min": st.column_config.NumberColumn("Pensja Min", format="%d", width="small"),
                "salary_max": st.column_config.NumberColumn("Pensja Max", format="%d", width="small"),
                "currency": st.column_config.TextColumn("Waluta", width=60),
                "url": st.column_config.LinkColumn("Aplikuj", display_text="Otworz", width=70)
            }

            display_columns_pl = [
                'date_added', 'kategoria', 'title', 'company_name', 'location', 
                'remote', 'contract_type', 'salary_min', 'salary_max', 'currency', 'url'
            ]
            existing_cols_pl = [col for col in display_columns_pl if col in df_pl.columns]

            if 'date_added' in existing_cols_pl:
                df_pl = df_pl.sort_values(by='date_added', ascending=False)

            st.dataframe(
                df_pl[existing_cols_pl], 
                column_config=column_config_pl, 
                hide_index=True, 
                use_container_width=True
            )
            
            st.markdown("---")
            st.subheader("📊 Analiza Kategorii")
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Liczba ofert w danej kategorii**")
                oferty_kategorie = df_pl['kategoria'].value_counts().reset_index()
                oferty_kategorie.columns = ['Kategoria', 'Liczba ofert']
                st.bar_chart(data=oferty_kategorie, x='Kategoria', y='Liczba ofert')
                
            with col2:
                st.write("**Średnia pensja w kategorii (PLN)**")
                srednia_kategorie = df_pl.dropna(subset=['salary_avg']).groupby('kategoria')['salary_avg'].mean().reset_index()
                st.bar_chart(data=srednia_kategorie, x='kategoria', y='salary_avg')

            st.markdown("---")
            st.subheader("🗺️ Mapa Ofert Pracy")

            coords = {
                'Warszawa': [52.2297, 21.0122], 'Kraków': [50.0647, 19.9450],
                'Wrocław': [51.1079, 17.0385], 'Poznań': [52.4064, 16.9252],
                'Gdańsk': [54.3520, 18.6466], 'Katowice': [50.2649, 19.0238],
                'Łódź': [51.7592, 19.4560], 'Szczecin': [53.4285, 14.5528],
                'Lublin': [51.2465, 22.5684], 'Białystok': [53.1325, 23.1688]
            }
            
            map_data = []
            for index, row in df_pl.iterrows():
                miasto = str(row['location']).split(',')[0].strip()
                if miasto in coords:
                    map_data.append({'lat': coords[miasto][0], 'lon': coords[miasto][1]})
            
            df_map = pd.DataFrame(map_data)
            if not df_map.empty:
                st.map(df_map, zoom=5)
            else:
                st.info("Brak precyzyjnych danych o miastach do narysowania mapy.")

    except Exception as e:
        st.error(f"Błąd ładowania danych z Polski: {e}")

# --- Zakładka 3: Technologie ---
with tab_tech:
    st.header("🔥 Top 10 Technologii w Polsce")
    try:
        df_tech = fetch_poland_data()
        
        if 'technologie' in df_tech.columns:
            tech_series = df_tech['technologie'].dropna().astype(str).str.split(',').explode()
            tech_series = tech_series.str.strip().str.upper()
            tech_series = tech_series[(tech_series != '') & (tech_series != 'NAN') & (tech_series != 'NONE')]
            
            top_10_tech = tech_series.value_counts().head(10)
            st.bar_chart(top_10_tech)
        else:
            st.warning("Brak kolumny 'technologie' w bazie.")
    except Exception as e:
        st.error(f"Błąd ładowania technologii: {e}")

# --- Zakładka 4: Estymator ML ---
with tab_ai:
    st.header("🤖 Estymator Wynagrodzeń ML")
    st.markdown("---")
    
    try:
        model = joblib.load('salary_model.pkl')
        model_columns = joblib.load('model_columns.pkl')

        col1, col2, col3 = st.columns(3)

        with col1:
            user_kategoria = st.selectbox("Kategoria IT", ["Backend", "Frontend", "Data", "DevOps", "Testing", "Fullstack", "Mobile", "Security"])
            user_seniority = st.selectbox("Seniority", ["Junior", "Mid", "Senior"])
            user_contract = st.selectbox("Typ umowy", ["B2B", "UoP", "Inna"]) # <--- NOWE POLE

        with col2:
            user_location = st.selectbox("Lokalizacja", ["Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań", "Łódź", "Katowice", "Zdalnie"])
            user_remote = st.selectbox("Praca w pełni zdalna", ["True", "False"])

        with col3:
            st.write("Stack:")
            user_python = st.checkbox("Python")
            user_java = st.checkbox("Java / Spring")
            user_data = st.checkbox("SQL / Data / BI")
            user_cloud = st.checkbox("AWS / Docker / Cloud")
            user_frontend = st.checkbox("React / Angular / Vue")

        if st.button("Oblicz estymację", type="primary"):
            input_data = pd.DataFrame({
                'kategoria': [user_kategoria],
                'location': [user_location],
                'seniority': [user_seniority],
                'remote': [user_remote],
                'contract_type': [user_contract], 
                'tech_python': [1 if user_python else 0],
                'tech_java': [1 if user_java else 0],
                'tech_data_sql': [1 if user_data else 0],
                'tech_cloud': [1 if user_cloud else 0],
                'tech_frontend': [1 if user_frontend else 0]
            })

            input_encoded = pd.get_dummies(input_data)
            input_encoded = input_encoded.reindex(columns=model_columns, fill_value=0)

            prediction = model.predict(input_encoded)[0]

            st.success(f"Estymowane widełki: **{prediction:,.0f} PLN**")
            st.caption(f"MAE modelu XGBoost: ~4723 PLN.")

    except FileNotFoundError:
        st.error("Brak plików modelu (salary_model.pkl / model_columns.pkl).")