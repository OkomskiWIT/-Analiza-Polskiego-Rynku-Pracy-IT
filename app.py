import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import folium
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from streamlit_folium import st_folium
from datetime import datetime

DB_URL = st.secrets["DB_URL"]

# ==========================================
# FUNKCJE POBIERANIA I CACHE
# ==========================================
@st.cache_data(ttl=21600)
def fetch_global_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM job_offers;", engine)
    df = df.reset_index(drop=True)
    if 'Lp.' not in df.columns:
        df.insert(0, 'Lp.', range(1, len(df) + 1))
        
    if 'title' in df.columns:
        df['title'] = df['title'].apply(lambda x: str(x)[0].upper() + str(x)[1:] if pd.notna(x) and str(x).strip() else x)
        
    if 'remote' in df.columns:
        df['remote'] = df['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
    return df

@st.cache_data(ttl=21600)
def fetch_poland_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM poland_job_offers;", engine)
    
    if not df.empty:
        df['salary_avg'] = (df['salary_min'] + df['salary_max']) / 2
        
        # --- ZAAWANSOWANA DEDUPLIKACJA ---
        if 'date_added' in df.columns:
            df = df.sort_values(by='date_added', ascending=False)
            
        df['temp_title'] = df['title'].astype(str).str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
        df['temp_company'] = df['company_name'].astype(str).str.lower().str.replace(r'(sp\. z o\.o\.|spółka|inc\.|ltd\.|sa|s\.a\.|[^a-z0-9])', '', regex=True)
        df = df.drop_duplicates(subset=['temp_title', 'temp_company'], keep='first')
        df = df.drop(columns=['temp_title', 'temp_company'])
        # ---------------------------------
        
        if 'title' in df.columns:
            df['title'] = df['title'].apply(lambda x: str(x)[0].upper() + str(x)[1:] if pd.notna(x) and str(x).strip() else x)
            
        if 'remote' in df.columns:
            df['remote'] = df['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
            
        if 'location' in df.columns:
            df['location'] = df['location'].astype(str).str.lstrip(', ').str.replace(r',\s*(,)+', ',', regex=True)
            
    return df.reset_index(drop=True)

@st.cache_data(ttl=21600)
def get_tech_counts(df):
    if 'technologie' not in df.columns:
        return pd.Series(dtype=int)
    tech_series = df['technologie'].dropna().astype(str).str.split(',').explode()
    tech_series = tech_series.str.strip().str.upper()
    tech_series = tech_series[(tech_series != '') & (tech_series != 'NAN') & (tech_series != 'NONE')]
    return tech_series.value_counts()

@st.cache_data(ttl=21600)
def generate_wordcloud_image(tech_counts):
    return WordCloud(
        width=800, height=500, background_color='white', 
        colormap='viridis', max_words=100
    ).generate_from_frequencies(tech_counts)

@st.cache_data(ttl=21600)
def prepare_nlp_matrix(df):
    df_clean = df.copy()
    df_clean['technologie'] = df_clean['technologie'].fillna('')
    df_clean['title'] = df_clean['title'].fillna('')
    df_clean['kategoria'] = df_clean['kategoria'].fillna('')
    df_clean['tekst_do_analizy'] = df_clean['kategoria'] + " " + df_clean['title'] + " " + df_clean['technologie']
    corpus = df_clean['tekst_do_analizy'].tolist()
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(corpus)
    return vectorizer, tfidf_matrix, df_clean

@st.cache_resource
def load_ml_model():
    try:
        model = joblib.load('salary_model.pkl')
        model_columns = joblib.load('model_columns.pkl')
        return model, model_columns
    except Exception:
        return None, None

def build_interactive_map(df, max_pins=2000):
    m = folium.Map(location=[52.0693, 19.4803], zoom_start=6, tiles="CartoDB positron")
    marker_cluster = MarkerCluster().add_to(m)
    grouped_offers = {}
    laczna_liczba_wczytanych_ofert = 0

    for row in df.itertuples():
        if laczna_liczba_wczytanych_ofert >= max_pins:
            break 
        coords_raw = getattr(row, 'coordinates', None)
        if coords_raw is None or (isinstance(coords_raw, float) and pd.isna(coords_raw)):
            continue
        try:
            coords_list = []
            if isinstance(coords_raw, str):
                if coords_raw.strip() in ['[]', '']: continue
                try: coords_list = json.loads(coords_raw)
                except: coords_list = json.loads(coords_raw.replace("'", '"'))
            elif isinstance(coords_raw, list):
                coords_list = coords_raw
            else: continue
                
            for loc in coords_list:
                if laczna_liczba_wczytanych_ofert >= max_pins: break
                try:
                    lat_val = loc.get('lat')
                    lon_val = loc.get('lon')
                    if lat_val is None or lon_val is None: continue
                    lat = float(lat_val)
                    lon = float(lon_val)
                    if pd.isna(lat) or pd.isna(lon): continue
                    if not (49.0 <= lat <= 55.0 and 14.0 <= lon <= 25.0): continue
                except (ValueError, TypeError):
                    continue
                    
                coord_key = (lat, lon)
                ulica = loc.get('street', '')
                miasto = loc.get('city', '')
                adres = f"{ulica}, {miasto}" if ulica else miasto
                zarobki = "Brak widełek"
                if pd.notna(getattr(row, 'salary_min', None)) and pd.notna(getattr(row, 'salary_max', None)):
                    zarobki = f"{int(row.salary_min)} - {int(row.salary_max)} {row.currency}"
                
                if coord_key not in grouped_offers:
                    grouped_offers[coord_key] = {'adres': adres, 'firmy': set(), 'oferty_html': []}
                
                grouped_offers[coord_key]['firmy'].add(row.company_name)
                offer_html = f"<li style='margin-bottom: 5px;'><b>{row.title}</b><br>💰 {zarobki} | <a href='{row.url}' target='_blank'>Aplikuj</a></li>"
                grouped_offers[coord_key]['oferty_html'].append(offer_html)
                laczna_liczba_wczytanych_ofert += 1
        except Exception:
            pass

    for (lat, lon), data in grouped_offers.items():
        nazwy_firm = ", ".join(list(data['firmy']))
        liczba_ofert = len(data['oferty_html'])
        
        limit_wyswietlania = 8
        lista_ofert_html = "".join(data['oferty_html'][:limit_wyswietlania])
        
        if liczba_ofert > limit_wyswietlania:
            lista_ofert_html += f"<li style='margin-top: 8px; color: #64748B;'><i>...oraz {liczba_ofert - limit_wyswietlania} innych ofert. Użyj filtrów w panelu bocznym.</i></li>"

        popup_html = f"""
        <div style="min-width: 250px; font-family: Arial, sans-serif;">
            <b style="font-size: 14px; color: #2E66F6;">🏢 {nazwy_firm}</b><br>
            📍 {data['adres']}<br>
            <i>Ofert w tym miejscu: <b>{liczba_ofert}</b></i>
            <hr style="margin: 5px 0;">
            <div style="max-height: 220px; overflow-y: auto; background-color: #f8fafc; padding: 8px; border-radius: 6px;">
                <ul style="padding-left: 20px; margin: 0; font-size: 12px; line-height: 1.4;">
                    {lista_ofert_html}
                </ul>
            </div>
        </div>
        """
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=f"{nazwy_firm} ({liczba_ofert} ofert)",
            icon=folium.Icon(color="#2E66F6", icon="info-sign")
        ).add_to(marker_cluster)
            
    return m, laczna_liczba_wczytanych_ofert, []

# ==========================================
# WSTRZYKNIĘCIE CSS (RESPONSYWNOŚĆ I STYL)
# ==========================================
def apply_custom_css():
    st.markdown("""
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        div[data-testid="stExpander"] {
            border-radius: 12px !important;
            border: 1px solid #E2E8F0 !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
            background-color: #FFFFFF;
        }
        
        .offers-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1.5rem;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        
        .offer-card {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.5rem;
            transition: transform 0.2s, box-shadow 0.2s;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-height: 320px;
        }
        .offer-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            border-color: #2E66F6;
        }
        .offer-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #1E293B;
            margin-bottom: 0.5rem;
            line-height: 1.3;
            text-transform: capitalize;
        }
        .offer-company {
            font-size: 0.95rem;
            color: #64748B;
            margin-bottom: 1rem;
            font-weight: 500;
        }
        .offer-salary {
            font-size: 1.1rem;
            font-weight: 700;
            color: #10B981;
            margin-bottom: 1rem;
            background-color: #ECFDF5;
            display: inline-block;
            padding: 0.3rem 0.6rem;
            border-radius: 6px;
        }
        .badge {
            display: inline-block;
            padding: 0.3rem 0.6rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            background-color: #F1F5F9;
            color: #475569;
            margin-right: 0.4rem;
            margin-bottom: 0.4rem;
        }
        .badge.remote { background-color: #DBEAFE; color: #1D4ED8; }
        .badge.b2b { background-color: #FEF3C7; color: #B45309; }
        
        .offer-meta {
            font-size: 0.8rem;
            color: #94A3B8;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
            display: flex;
            justify-content: space-between;
            border-top: 1px solid #F1F5F9;
            padding-top: 0.8rem;
        }
        
        .apply-btn {
            display: block;
            text-align: center;
            background-color: #2E66F6;
            color: white !important;
            padding: 0.7rem;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            margin-top: 0.5rem;
            transition: background-color 0.2s;
        }
        .apply-btn:hover { background-color: #1D4ED8; }
        
        @media (max-width: 768px) {
            .block-container { padding-top: 2rem !important; padding-left: 1rem !important; padding-right: 1rem !important; }
        }
        </style>
    """, unsafe_allow_html=True)

# ==========================================
# START APLIKACJI I GŁÓWNY PANEL
# ==========================================
st.set_page_config(page_title="Rynek Pracy IT", layout="wide")
apply_custom_css()

with st.spinner("Ładowanie danych z bazy..."):
    df_global_main = fetch_global_data()
    df_pl_main = fetch_poland_data()
    ml_model, ml_columns = load_ml_model()

st.title("Rynek Pracy IT w Polsce i na Świecie 🌍")

# --- PANEL BOCZNY (FILTRY GLOBALNE) ---
with st.sidebar:
    st.header("🎛️ Filtry Segmentowe")
    st.markdown("Działają na tabelę ofert, mapę oraz statystyki z Polski.")
    
    kategorie_lista = ['Wszystkie'] + sorted(df_pl_main['kategoria'].unique().tolist())
    wybrana_kategoria = st.selectbox("Kategoria IT", kategorie_lista)
    tylko_zdalnie = st.checkbox("Tylko praca w pełni zdalna")
    
    st.markdown("---")
    st.subheader("⚙️ System")
    if st.button("🔄 Odśwież bazę danych", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# APLIKOWANIE FILTRÓW DO GŁÓWNEJ RAMKI PL
df_pl_filtered = df_pl_main.copy()

if wybrana_kategoria != 'Wszystkie':
    df_pl_filtered = df_pl_filtered[df_pl_filtered['kategoria'] == wybrana_kategoria]
if tylko_zdalnie:
    df_pl_filtered = df_pl_filtered[df_pl_filtered['remote'] == 'Tak']

# STRUKTURA ZAKŁADEK
tab_pl_oferty, tab_pl_analiza, tab_global, tab_tech, tab_ai, tab_nlp = st.tabs([
    "🇵🇱 Oferty (Polska)", 
    "📊 Analiza i Mapa (PL)",
    "🌏 Rynek Globalny", 
    "🔥 Top Technologie", 
    "🤖 Kalkulator ML",
    "🎯 Dopasuj Ofertę (NLP)"
])

# --- Zakładka 1: Rynek Polski (OFERTY) ---
with tab_pl_oferty:
    metric_placeholder = st.container()

    wyszukiwarka = st.text_input("🔍 Wyszukiwarka ofert:", placeholder="Wpisz słowo kluczowe (np. Python, Senior, Android, Comarch...)")

    if wyszukiwarka:
        w_low = wyszukiwarka.lower()
        mask = (
            df_pl_filtered['title'].str.lower().str.contains(w_low, na=False) |
            df_pl_filtered['company_name'].str.lower().str.contains(w_low, na=False) |
            df_pl_filtered['technologie'].str.lower().str.contains(w_low, na=False)
        )
        df_pl_filtered = df_pl_filtered[mask]

    with metric_placeholder:
        kpi1, kpi2 = st.columns(2)
        with kpi1:
            st.metric("📊 Aktywne oferty (po filtrach)", value=f"{len(df_pl_filtered):,}".replace(',', ' '))
        with kpi2:
            praca_zdalna = len(df_pl_filtered[df_pl_filtered['remote'] == 'Tak'])
            st.metric("🏠 Ofert zdalnych (po filtrach)", value=f"{praca_zdalna:,}".replace(',', ' '))
        st.markdown("---")

    sort_option = st.selectbox("Sortuj oferty po:", [
        "Najnowsze", 
        "Najwyższych zarobkach", 
        "Najniższych zarobkach",
        "Alfabetycznie"
    ], index=0)
    
    if sort_option == "Najnowsze" and 'date_added' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='date_added', ascending=False)
    elif sort_option == "Najwyższych zarobkach" and 'salary_max' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='salary_max', ascending=False, na_position='last')
    elif sort_option == "Najniższych zarobkach" and 'salary_min' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='salary_min', ascending=True, na_position='last')
    elif sort_option == "Alfabetycznie" and 'company_name' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='company_name', ascending=True)

    if not df_pl_filtered.empty:
        html_content = '<div class="offers-grid">\n'
        for idx, row in df_pl_filtered.head(120).iterrows():
            zarobki = f"{int(row['salary_min'])} - {int(row['salary_max'])} {row['currency']}" if pd.notna(row['salary_min']) else "Brak podanych widełek"
            
            zdalnie_badge = '<span class="badge remote">🌍 Praca Zdalna</span>' if row['remote'] == 'Tak' else ''
            umowa_badge = f'<span class="badge b2b">📄 {row["contract_type"]}</span>' if row['contract_type'] and str(row['contract_type']).strip() != 'Inna' else ''
            kategoria_badge = f'<span class="badge">💻 {row["kategoria"]}</span>'
            lokalizacja_badge = f'<span class="badge">📍 {row["location"]}</span>'
            
            date_str = "Brak danych"
            try:
                if 'date_added' in row and pd.notna(row['date_added']):
                    date_str = pd.to_datetime(row['date_added']).strftime('%Y-%m-%d')
            except:
                pass
            
            html_content += (
                '<div class="offer-card">\n'
                '<div>\n'
                f'<div class="offer-title">{str(row["title"]).replace("<", "").replace(">", "")}</div>\n'
                f'<div class="offer-company">🏢 {str(row["company_name"]).replace("<", "").replace(">", "")}</div>\n'
                f'<div class="offer-salary">💰 {zarobki}</div>\n'
                '<div style="margin-bottom: 0.5rem; line-height: 2;">\n'
                f'{kategoria_badge} {lokalizacja_badge} {zdalnie_badge} {umowa_badge}\n'
                '</div>\n'
                '</div>\n'
                '<div>\n'
                '<div class="offer-meta">\n'
                f'<span>📅 Dodano: {date_str}</span>\n'
                '</div>\n'
                f'<a href="{row["url"]}" target="_blank" class="apply-btn">Zobacz i Aplikuj</a>\n'
                '</div>\n'
                '</div>\n'
            )

        html_content += '</div>'
        if len(df_pl_filtered) > 120:
            html_content += f'<p style="text-align:center; color:#64748B;">Pokazuję pierwsze 120 z {len(df_pl_filtered)} wyników. Skorzystaj z filtrów bocznych i wyszukiwarki, aby zawęzić listę.</p>'
            
        st.markdown(html_content, unsafe_allow_html=True)
    else:
        st.info("Brak ofert spełniających kryteria wyszukiwania i filtrowania.")

# --- Zakładka 2: Rynek Polski (ANALIZA I MAPA) ---
with tab_pl_analiza:
    st.header("Dane Statystyczne i Mapa")
    st.caption("Poniższe statystyki reagują na Twoje wyszukiwanie i filtry wybrane w panelu bocznym i pierwszej zakładce.")
    
    kpi1_a, kpi2_a, kpi3_a = st.columns(3)
    with kpi1_a:
        st.metric("📊 Aktywne oferty", value=f"{len(df_pl_filtered):,}".replace(',', ' '))
    with kpi2_a:
        mediana_a = int(df_pl_filtered['salary_avg'].median()) if not df_pl_filtered.empty and not pd.isna(df_pl_filtered['salary_avg'].median()) else 0
        st.metric("💰 Mediana wynagrodzeń", value=f"{mediana_a:,} PLN".replace(',', ' '))
    with kpi3_a:
        praca_zdalna_a = len(df_pl_filtered[df_pl_filtered['remote'] == 'Tak'])
        st.metric("🏠 Ofert zdalnych", value=f"{praca_zdalna_a:,}".replace(',', ' '))

    st.markdown("---")
    
    if not df_pl_filtered.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Rozkład kategorii (w wybranym widoku)**")
            oferty_kategorie = df_pl_filtered['kategoria'].value_counts().reset_index()
            oferty_kategorie.columns = ['Kategoria', 'Liczba ofert']
            st.bar_chart(data=oferty_kategorie, x='Kategoria', y='Liczba ofert')
        with col2:
            st.write("**Mediana pensji w kategorii (PLN)**")
            mediana_kategorie = df_pl_filtered.dropna(subset=['salary_avg']).groupby('kategoria')['salary_avg'].median().reset_index()
            if not mediana_kategorie.empty:
                st.bar_chart(data=mediana_kategorie, x='kategoria', y='salary_avg')
            else:
                st.info("Brak danych finansowych do wygenerowania wykresu.")
    else:
        st.info("Brak danych po nałożeniu filtrów.")

    st.markdown("---")
    st.subheader("🗺️ Interaktywna Mapa Ofert Pracy")
    if st.button("🗺️ Załaduj i pokaż mapę", type="primary"):
        with st.spinner("Generowanie mapy w pamięci serwera... (bez zapisu na dysk)"):
            m, laczna_liczba_pinezek, bledy_log = build_interactive_map(df_pl_filtered)

        if laczna_liczba_pinezek > 0:
            st.success(f"Sukces! Załadowano próbkę {laczna_liczba_pinezek} ofert na mapę.")
            # ZMIANA NA ST_FOLIUM - brak pliku dyskowego temp_map.html!
            st_folium(m, use_container_width=True, height=650, returned_objects=[])
        else:
            st.warning("Brak ofert z poprawnymi danymi geograficznymi dla tych filtrów.")

# --- Zakładka 3: Rynek Globalny ---
with tab_global:
    st.header("Oferty Globalne")
    try:
        display_columns = ['Lp.', 'title', 'company_name', 'location', 'remote', 'url']
        existing_cols = [col for col in display_columns if col in df_global_main.columns]
        column_config = {
            "Lp.": st.column_config.NumberColumn("Lp.", width=50),
            "title": st.column_config.TextColumn("Stanowisko", width="large"),
            "company_name": st.column_config.TextColumn("Firma", width="medium"),
            "location": st.column_config.TextColumn("Lokalizacja", width="medium"),
            "remote": st.column_config.TextColumn("Zdalnie", width=70),
            "url": st.column_config.LinkColumn("Aplikuj", display_text="Otworz", width=70)
        }
        st.metric("Liczba ofert globalnych", len(df_global_main))
        st.dataframe(df_global_main[existing_cols], column_config=column_config, hide_index=True, use_container_width=True)
    except Exception as e:
        st.error(f"Błąd ładowania danych globalnych: {e}")

# --- Zakładka 4: Technologie ---
with tab_tech:
    st.header("🔥 Analiza Technologii i Wymagań na Rynku")
    st.caption("Poniższe dane analizują cały rynek, ignorując filtry z zakładki 'Oferty'.")
    try:
        tech_counts = get_tech_counts(df_pl_main)
        if not tech_counts.empty:
            col1, col2 = st.columns([2, 1]) 
            with col1:
                st.subheader("☁️ Chmura pożądanych technologii")
                with st.spinner("Generowanie grafiki wektorowej..."):
                    wordcloud = generate_wordcloud_image(tech_counts)
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.imshow(wordcloud, interpolation='bilinear')
                    ax.axis('off')
                    st.pyplot(fig)
            with col2:
                st.subheader("📊 Top 15 Stacku")
                df_top = tech_counts.head(15).reset_index()
                df_top.columns = ['Technologia', 'Liczba ofert']
                st.dataframe(df_top, hide_index=True, use_container_width=True)
                
            st.markdown("---")
            st.subheader("📈 Wykres słupkowy (Top 10)")
            st.bar_chart(tech_counts.head(10))
        else:
            st.warning("Brak danych po oczyszczeniu kolumny technologii.")
    except Exception as e:
        st.error(f"Błąd ładowania technologii: {e}")

# --- Zakładka 5: Estymator ML ---
with tab_ai:
    st.header("🤖 Estymator Wynagrodzeń ML")
    st.markdown("---")
    try:
        if ml_model is not None and ml_columns is not None:
            col1, col2, col3 = st.columns(3)
            with col1:
                user_kategoria = st.selectbox("Kategoria IT", [
                    "Backend", "Frontend", "Fullstack", "Data & AI", "DevOps & Cloud", 
                    "Testing", "Architecture", "Project & Product Management", 
                    "Business / System Analysis", "Cybersecurity", "UX/UI Design", 
                    "ERP / CRM", "Mobile", "Game Development", "IT Support & Administration", "Inne"
                ])
                user_seniority = st.selectbox("Seniority", ["Junior", "Mid", "Senior"])
                user_contract = st.selectbox("Typ umowy", ["B2B", "UoP", "Inna"])
            
            with col2:
                user_location = st.selectbox("Lokalizacja (ML)", ["Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań", "Łódź", "Katowice", "Zdalnie"])
                user_remote = st.selectbox("Praca w pełni zdalna", ["True", "False"])
            
            with col3:
                st.write("**Główny Stack Technologiczny:**")
                user_python = st.checkbox("Python / Django / FastAPI")
                user_java = st.checkbox("Java / Spring")
                user_csharp = st.checkbox("C# / .NET")
                user_php_ruby = st.checkbox("PHP / Ruby")
                user_frontend = st.checkbox("Frontend (JS/TS/React/Vue)")
                user_data = st.checkbox("Data / SQL / ML / AI")
                user_cloud = st.checkbox("DevOps / Cloud / Docker")
                user_mobile = st.checkbox("Mobile (Swift/Kotlin/Flutter)")
                user_cpp = st.checkbox("C++ / GameDev")
                user_testing = st.checkbox("Testing / QA")
                user_erp = st.checkbox("ERP / CRM (SAP/Salesforce)")

            if st.button("Oblicz estymację", type="primary", use_container_width=True):
                input_data = pd.DataFrame({
                    'kategoria': [user_kategoria], 'location': [user_location], 'seniority': [user_seniority],
                    'remote': [user_remote], 'contract_type': [user_contract], 'tech_python': [1 if user_python else 0],
                    'tech_java': [1 if user_java else 0], 'tech_data_sql': [1 if user_data else 0],
                    'tech_cloud': [1 if user_cloud else 0], 'tech_frontend': [1 if user_frontend else 0],
                    'tech_csharp_net': [1 if user_csharp else 0], 'tech_cpp_gamedev': [1 if user_cpp else 0],
                    'tech_mobile': [1 if user_mobile else 0], 'tech_php_ruby': [1 if user_php_ruby else 0],
                    'tech_testing_qa': [1 if user_testing else 0], 'tech_erp_crm': [1 if user_erp else 0]
                })

                input_encoded = pd.get_dummies(input_data)
                input_encoded = input_encoded.reindex(columns=ml_columns, fill_value=0)
                prediction = ml_model.predict(input_encoded)[0]

                st.success(f"Estymowane widełki: **{prediction:,.0f} PLN**")

            st.markdown("---")
            st.subheader("🧠 Wyjaśnialne AI (Co wpływa na pensję?)")
            if hasattr(ml_model, 'feature_importances_'):
                importances = ml_model.feature_importances_
                df_importance = pd.DataFrame({'Cecha': ml_columns, 'Waga (%)': importances * 100})
                df_importance = df_importance.sort_values(by='Waga (%)', ascending=False).head(10)
                
                def format_label(col_name):
                    translations = {
                        'seniority_': 'Poziom: ', 'kategoria_': 'Kategoria: ', 'location_': 'Lokalizacja: ', 'contract_type_': 'Umowa: ',
                        'tech_python': 'Tech: Python', 'tech_java': 'Tech: Java', 'tech_data_sql': 'Tech: Data/SQL/AI',
                        'tech_cloud': 'Tech: DevOps/Cloud', 'tech_frontend': 'Tech: Frontend', 'tech_csharp_net': 'Tech: C#/.NET',
                        'tech_cpp_gamedev': 'Tech: C++/GameDev', 'tech_mobile': 'Tech: Mobile', 'tech_php_ruby': 'Tech: PHP/Ruby',
                        'tech_testing_qa': 'Tech: QA/Testing', 'tech_erp_crm': 'Tech: ERP/CRM'
                    }
                    for eng, pl in translations.items():
                        if col_name.startswith(eng):
                            clean_name = col_name.replace(eng, pl)
                            return clean_name[:35] + "..." if len(clean_name) > 35 else clean_name
                    return col_name
                
                df_importance['Cecha_Display'] = df_importance['Cecha'].apply(format_label)
                fig_ai, ax_ai = plt.subplots(figsize=(10, 5))
                ax_ai.barh(df_importance['Cecha_Display'][::-1], df_importance['Waga (%)'][::-1], color='#ff4b4b')
                ax_ai.set_xlabel('Wpływ na ostateczną pensję (%)')
                ax_ai.set_title('Top 10 czynników podbijających wycenę kandydata')
                plt.tight_layout() 
                st.pyplot(fig_ai)
            else:
                st.info("Załadowany model nie wspiera wyodrębniania ważności cech.")
        else:
            st.error("Błąd: Nie udało się załadować plików modelu ML.")
    except Exception as e:
        st.error(f"Błąd analizy modelu: {e}")

# --- Zakładka 6: Dopasowanie Ofert (NLP) ---   
with tab_nlp:
    st.header("🎯 Inteligentne Dopasowanie Ofert (NLP)")
    try:
        if not df_pl_main.empty:
            vectorizer, tfidf_matrix, df_nlp = prepare_nlp_matrix(df_pl_main)
            user_skills = st.text_area("Wpisz swoje technologie i doświadczenie (np. 'Python, SQL, AWS, Docker'):", height=100)
            
            if st.button("Znajdź idealne oferty", type="primary", use_container_width=True):
                if len(user_skills) < 5:
                    st.warning("Wpisz więcej informacji, aby algorytm miał na czym pracować!")
                else:
                    with st.spinner('Obliczanie macierzy podobieństwa...'):
                        user_tfidf = vectorizer.transform([user_skills])
                        cosine_similarities = cosine_similarity(user_tfidf, tfidf_matrix).flatten()
                        top_5_indices = cosine_similarities.argsort()[-5:][::-1]
                        
                        st.subheader("Oto 5 najlepszych dopasowań:")
                        for i, idx in enumerate(top_5_indices):
                            score = cosine_similarities[idx]
                            row = df_nlp.iloc[idx]
                            if score > 0.05:
                                with st.expander(f"{i+1}. {row['title']} w {row['company_name']} (Dopasowanie: {score*100:.1f}%)"):
                                    st.write(f"**Lokalizacja:** {row['location']} | **Zdalnie:** {'Tak' if row['remote'] else 'Nie'}")
                                    st.write(f"**Umowa:** {row['contract_type']}")
                                    zarobki = f"{int(row['salary_min'])} - {int(row['salary_max'])} {row['currency']}" if pd.notna(row['salary_min']) else "Brak widełek"
                                    st.write(f"**Zarobki:** {zarobki}")
                                    st.write(f"**Wymagane technologie:** {row['technologie']}")
                                    st.markdown(f"[🔗 Kliknij, aby aplikować]({row['url']})")
                            else:
                                if i == 0:
                                    st.info("Brak silnego dopasowania w bazie dla podanych umiejętności.")
                                break
    except Exception as e:
        st.error(f"Błąd modułu NLP: {e}")