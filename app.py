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
    if 'remote' in df.columns:
        df['remote'] = df['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
    return df

@st.cache_data(ttl=21600)
def fetch_poland_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM poland_job_offers;", engine)
    
    if not df.empty:
        df['salary_avg'] = (df['salary_min'] + df['salary_max']) / 2
        
        if 'remote' in df.columns:
            df['remote'] = df['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
            
        if 'location' in df.columns:
            df['location'] = df['location'].astype(str).str.lstrip(', ').str.replace(r',\s*(,)+', ',', regex=True)
            
        if 'date_added' in df.columns:
            df = df.sort_values(by='date_added', ascending=False)
            
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
        lista_ofert_html = "".join(data['oferty_html'])
        popup_html = f"""
        <div style="min-width: 250px; font-family: Arial, sans-serif;">
            <b style="font-size: 14px; color: #0066cc;">🏢 {nazwy_firm}</b><br>
            📍 {data['adres']}<br>
            <i>Liczba ofert w tej lokalizacji: <b>{liczba_ofert}</b></i>
            <hr style="margin: 5px 0;">
            <div style="max-height: 200px; overflow-y: auto; background-color: #f9f9f9; padding: 5px; border-radius: 4px;">
                <ul style="padding-left: 20px; margin: 0; font-size: 12px;">{lista_ofert_html}</ul>
            </div>
        </div>
        """
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=f"{nazwy_firm} ({liczba_ofert} ofert)",
            icon=folium.Icon(color="blue", icon="info-sign")
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
        
        /* Zaokrąglone rogi kontenerów */
        div[data-testid="stExpander"] {
            border-radius: 12px !important;
            border: 1px solid #E2E8F0 !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
            background-color: #FFFFFF;
        }
        
        /* Siatka ofert - Grid */
        .offers-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1.5rem;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        
        /* Karta pojedynczej oferty */
        .offer-card {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.5rem;
            transition: transform 0.2s, box-shadow 0.2s;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-height: 280px;
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
        /* Odznaki (Badge) */
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
        
        /* Przycisk Aplikuj */
        .apply-btn {
            display: block;
            text-align: center;
            background-color: #2E66F6;
            color: white !important;
            padding: 0.7rem;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            margin-top: 1rem;
            transition: background-color 0.2s;
        }
        .apply-btn:hover { background-color: #1D4ED8; }
        
        /* Odstępy na mobile */
        @media (max-width: 768px) {
            .block-container { padding-top: 2rem !important; padding-left: 1rem !important; padding-right: 1rem !important; }
        }
        </style>
    """, unsafe_allow_html=True)

# ==========================================
# START APLIKACJI I GŁÓWNY PANEL
# ==========================================
st.set_page_config(page_title="Rynek Pracy IT", layout="wide", initial_sidebar_state="expanded")
apply_custom_css()

# Pobieranie danych
with st.spinner("Ładowanie danych z bazy..."):
    df_global_main = fetch_global_data()
    df_pl_main = fetch_poland_data()
    ml_model, ml_columns = load_ml_model()

# --- PANEL BOCZNY (FILTRY GLOBALNE) ---
with st.sidebar:
    st.header("🎛️ Filtruj Wyniki")
    st.markdown("Filtry działają na tabelę ofert, mapę oraz statystyki z Polski.")
    
    kategorie_lista = ['Wszystkie'] + sorted(df_pl_main['kategoria'].unique().tolist())
    wybrana_kategoria = st.selectbox("Kategoria IT", kategorie_lista)
    tylko_zdalnie = st.checkbox("Tylko praca w pełni zdalna")
    
    st.markdown("---")
    st.subheader("⚙️ System")
    if st.button("🔄 Wymuś odświeżenie bazy", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# APLIKOWANIE FILTRÓW DO GŁÓWNEJ RAMKI PL
df_pl_filtered = df_pl_main.copy()
if wybrana_kategoria != 'Wszystkie':
    df_pl_filtered = df_pl_filtered[df_pl_filtered['kategoria'] == wybrana_kategoria]
if tylko_zdalnie:
    df_pl_filtered = df_pl_filtered[df_pl_filtered['remote'] == 'Tak']

st.title("Analityka Rynku Pracy IT")

tab_pl, tab_global, tab_tech, tab_ai, tab_nlp = st.tabs([
    "🇵🇱 Rynek Polski & Zarobki", 
    "🌏 Rynek Globalny", 
    "🔥 Top Technologie", 
    "🤖 Kalkulator ML",
    "🎯 Dopasuj Ofertę (NLP)"
])

# --- Zakładka 1: Rynek Polski ---
with tab_pl:
    # Sekcja KPI (Wskaźniki biznesowe)
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        st.metric("📊 Aktywne oferty (po filtrach)", value=f"{len(df_pl_filtered):,}".replace(',', ' '))
    with kpi2:
        srednia = int(df_pl_filtered['salary_avg'].mean()) if not df_pl_filtered.empty and not pd.isna(df_pl_filtered['salary_avg'].mean()) else 0
        st.metric("💰 Średnia wynagrodzeń", value=f"{srednia:,} PLN".replace(',', ' '))
    with kpi3:
        praca_zdalna = len(df_pl_filtered[df_pl_filtered['remote'] == 'Tak'])
        st.metric("🏠 Ofert zdalnych", value=f"{praca_zdalna:,}".replace(',', ' '))

    st.markdown("---")
    
    # Przełącznik sortowania (zastępstwo za klikanie w nagłówki tabeli)
    sort_option = st.selectbox("Sortuj oferty po:", ["Najnowsze dacie dodania", "Najwyższych zarobkach (max)"], index=0)
    if sort_option == "Najnowsze dacie dodania" and 'date_added' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='date_added', ascending=False)
    elif sort_option == "Najwyższych zarobkach (max)" and 'salary_max' in df_pl_filtered.columns:
        df_pl_filtered = df_pl_filtered.sort_values(by='salary_max', ascending=False)

    # GENERATOR HTML DLA KAFELKÓW
    if not df_pl_filtered.empty:
        html_content = '<div class="offers-grid">'
        # Limit do 120 ofert, by nie zabić przeglądarki DOM-em (przy 5000+ ofert nikt i tak nie przewinie do końca)
        for idx, row in df_pl_filtered.head(120).iterrows():
            zarobki = f"{int(row['salary_min'])} - {int(row['salary_max'])} {row['currency']}" if pd.notna(row['salary_min']) else "Brak podanych widełek"
            
            zdalnie_badge = '<span class="badge remote">🌍 Praca Zdalna</span>' if row['remote'] == 'Tak' else ''
            umowa_badge = f'<span class="badge b2b">📄 {row["contract_type"]}</span>' if row['contract_type'] and str(row['contract_type']).strip() != 'Inna' else ''
            kategoria_badge = f'<span class="badge">💻 {row["kategoria"]}</span>'
            lokalizacja_badge = f'<span class="badge">📍 {row["location"]}</span>'
            
            html_content += f"""
            <div class="offer-card">
                <div>
                    <div class="offer-title">{str(row['title']).replace('<', '').replace('>', '')}</div>
                    <div class="offer-company">🏢 {str(row['company_name']).replace('<', '').replace('>', '')}</div>
                    <div class="offer-salary">💰 {zarobki}</div>
                    <div style="margin-bottom: 0.5rem; line-height: 2;">
                        {kategoria_badge}
                        {lokalizacja_badge}
                        {zdalnie_badge}
                        {umowa_badge}
                    </div>
                </div>
                <a href="{row['url']}" target="_blank" class="apply-btn">Zobacz i Aplikuj</a>
            </div>
            """
        html_content += '</div>'
        if len(df_pl_filtered) > 120:
            html_content += f'<p style="text-align:center; color:#64748B;">Pokazuję pierwsze 120 z {len(df_pl_filtered)} wyników. Użyj filtrów bocznych, aby zawęzić listę.</p>'
            
        st.markdown(html_content, unsafe_allow_html=True)
    else:
        st.info("Brak ofert spełniających kryteria filtrowania.")

    st.markdown("---")
    
    with st.expander("📊 Pokaż wykresy i analizę statystyczną (Dopasowane do filtrów)", expanded=False):
        if not df_pl_filtered.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Rozkład kategorii (w wybranym widoku)**")
                oferty_kategorie = df_pl_filtered['kategoria'].value_counts().reset_index()
                oferty_kategorie.columns = ['Kategoria', 'Liczba ofert']
                st.bar_chart(data=oferty_kategorie, x='Kategoria', y='Liczba ofert')
            with col2:
                st.write("**Średnia pensja (PLN)**")
                srednia_kategorie = df_pl_filtered.dropna(subset=['salary_avg']).groupby('kategoria')['salary_avg'].mean().reset_index()
                if not srednia_kategorie.empty:
                    st.bar_chart(data=srednia_kategorie, x='kategoria', y='salary_avg')
                else:
                    st.info("Brak danych finansowych do wygenerowania wykresu.")
        else:
            st.info("Brak danych po nałożeniu filtrów.")

    st.markdown("---")
    st.subheader("🗺️ Interaktywna Mapa Ofert Pracy")
    st.caption("Mapa prezentuje punkty zgodne z Twoimi ustawieniami w panelu bocznym.")
    if st.button("🗺️ Załaduj i pokaż mapę", type="primary"):
        with st.spinner("Przetwarzanie tysięcy koordynatów..."):
            # Mapa generowana z FILTROWANEJ ramki danych!
            m, laczna_liczba_pinezek, bledy_log = build_interactive_map(df_pl_filtered)

        if laczna_liczba_pinezek > 0:
            st.success(f"Sukces! Załadowano próbkę {laczna_liczba_pinezek} ofert na mapę.")
            import streamlit.components.v1 as components
            m.save("temp_map.html") 
            with open("temp_map.html", "r", encoding="utf-8") as f:
                html_data = f.read() 
            components.html(html_data, height=650)
        else:
            st.warning("Brak ofert z poprawnymi danymi geograficznymi dla tych filtrów.")

# --- Zakładka 2: Rynek Globalny ---
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

# --- Zakładka 3: Technologie ---
with tab_tech:
    st.header("🔥 Analiza Technologii i Wymagań na Rynku")
    st.caption("Poniższe dane analizują cały rynek, ignorując filtry boczne.")
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

# --- Zakładka 4: Estymator ML ---
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

# --- Zakładka 5: Dopasowanie Ofert (NLP) ---   
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