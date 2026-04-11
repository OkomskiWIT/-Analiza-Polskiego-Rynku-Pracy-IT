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
from streamlit_folium import st_folium, folium_static

DB_URL = st.secrets["DB_URL"]

# --- FUNKCJE POBIERAJĄCE DANE ---
@st.cache_data(ttl=3600)
def fetch_global_data():
    engine = create_engine(DB_URL)
    return pd.read_sql("SELECT * FROM job_offers;", engine)

@st.cache_data(ttl=3600)
def fetch_poland_data():
    engine = create_engine(DB_URL)
    return pd.read_sql("SELECT * FROM poland_job_offers;", engine)

# --- ZOPTYMALIZOWANA I BEZPIECZNA FUNKCJA BUDUJĄCA MAPĘ ---
def build_interactive_map(df, max_pins=2000):
    m = folium.Map(location=[52.0693, 19.4803], zoom_start=6, tiles="CartoDB positron")
    marker_cluster = MarkerCluster().add_to(m)
    
    grouped_offers = {}
    laczna_liczba_wczytanych_ofert = 0

    # KROK 1: Zbieranie i grupowanie danych z wierszy
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

                lat = loc.get('lat')
                lon = loc.get('lon')
                
                if lat and lon:
                    lat, lon = float(lat), float(lon)
                    
                    # Tworzymy unikalny klucz matematyczny dla tego biura
                    coord_key = (lat, lon)
                    
                    ulica = loc.get('street', '')
                    miasto = loc.get('city', '')
                    adres = f"{ulica}, {miasto}" if ulica else miasto
                    
                    zarobki = "Brak widełek"
                    if pd.notna(getattr(row, 'salary_min', None)) and pd.notna(getattr(row, 'salary_max', None)):
                        zarobki = f"{int(row.salary_min)} - {int(row.salary_max)} {row.currency}"
                    
                    # Jeśli ten punkt na mapie jeszcze nie istnieje, tworzymy go
                    if coord_key not in grouped_offers:
                        grouped_offers[coord_key] = {
                            'adres': adres,
                            'firmy': set(), # Używamy zbioru (set), żeby firmy się nie powtarzały
                            'oferty_html': []
                        }
                    
                    # Dodajemy ofertę do "pudełka" dla tego biurowca
                    grouped_offers[coord_key]['firmy'].add(row.company_name)
                    offer_html = f"<li style='margin-bottom: 5px;'><b>{row.title}</b><br>💰 {zarobki} | <a href='{row.url}' target='_blank'>Aplikuj</a></li>"
                    grouped_offers[coord_key]['oferty_html'].append(offer_html)
                    
                    laczna_liczba_wczytanych_ofert += 1
                    
        except Exception as e:
            pass # Ciche ignorowanie błędów pojedynczych JSONów

    # KROK 2: Rysowanie JEDNEJ pinezki na każdą grupę (biuro)
    for (lat, lon), data in grouped_offers.items():
        nazwy_firm = ", ".join(list(data['firmy']))
        liczba_ofert = len(data['oferty_html'])
        
        # Łączymy wszystkie oferty w jedną listę HTML
        lista_ofert_html = "".join(data['oferty_html'])
        
        # Scrollowany dymek (max-height: 200px; overflow-y: auto;)
        popup_html = f"""
        <div style="min-width: 250px; font-family: Arial, sans-serif;">
            <b style="font-size: 14px; color: #0066cc;">🏢 {nazwy_firm}</b><br>
            📍 {data['adres']}<br>
            <i>Liczba ofert w tej lokalizacji: <b>{liczba_ofert}</b></i>
            <hr style="margin: 5px 0;">
            <div style="max-height: 200px; overflow-y: auto; background-color: #f9f9f9; padding: 5px; border-radius: 4px;">
                <ul style="padding-left: 20px; margin: 0; font-size: 12px;">
                    {lista_ofert_html}
                </ul>
            </div>
        </div>
        """
        
        tooltip_text = f"{nazwy_firm} ({liczba_ofert} ofert)"
        
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=tooltip_text,
            icon=folium.Icon(color="blue", icon="info-sign")
        ).add_to(marker_cluster)
            
    return m, laczna_liczba_wczytanych_ofert, []

# --- KONFIGURACJA APLIKACJI ---
st.set_page_config(page_title="Rynek Pracy IT", layout="wide")
st.title("Analityka Rynku Pracy IT")

tab_pl, tab_global, tab_tech, tab_ai, tab_nlp = st.tabs([
    "Rynek Polski & Zarobki", 
    "Rynek Globalny", 
    "🔥 Top Technologie", 
    "🤖 Kalkulator ML",
    "🎯 Dopasuj Ofertę (NLP)"
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
                "contract_type": st.column_config.TextColumn("Umowa", width="small"),
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

            # ==========================================
            # MAPA Z SYSTEMEM LOGOWANIA
            # ==========================================
            st.markdown("---")
            st.subheader("🗺️ Interaktywna Mapa Ofert Pracy")

            if st.button("🗺️ Załaduj i pokaż mapę", type="primary"):
                with st.spinner("Przetwarzanie tysięcy koordynatów..."):
                    m, laczna_liczba_pinezek, bledy_log = build_interactive_map(df_pl)

                # DIAGNOSTYKA NA ŻYWO NA EKRANIE
                if laczna_liczba_pinezek > 0:
                    st.success(f"Sukces! Załadowano próbkę {laczna_liczba_pinezek} ofert na mapę, by zachować płynność działania przeglądarki.")
                    
                    import streamlit.components.v1 as components
                    m.save("temp_map.html") 
                    
                    with open("temp_map.html", "r", encoding="utf-8") as f:
                        html_data = f.read() 
                        
                    components.html(html_data, height=650)
                else:
                    st.error("Krytyczny błąd: Wygenerowano 0 pinezek.")

    except Exception as e:
        st.error(f"Błąd ładowania danych z Polski: {e}")

# --- Zakładka 3: Technologie ---
with tab_tech:
    st.header("🔥 Analiza Technologii i Wymagań na Rynku")
    st.markdown("Wizualizacja najczęściej wymaganych kompetencji przez pracodawców IT w Polsce.")
    
    try:
        df_tech = fetch_poland_data()
        
        if 'technologie' in df_tech.columns:
            # 1. Czyszczenie i przygotowanie danych
            tech_series = df_tech['technologie'].dropna().astype(str).str.split(',').explode()
            tech_series = tech_series.str.strip().str.upper()
            tech_series = tech_series[(tech_series != '') & (tech_series != 'NAN') & (tech_series != 'NONE')]
            
            # Zliczamy wystąpienia każdej technologii
            tech_counts = tech_series.value_counts()
            
            if not tech_counts.empty:
                col1, col2 = st.columns([2, 1]) 
                
                with col1:
                    st.subheader("☁️ Chmura pożądanych technologii")
                    with st.spinner("Generowanie grafiki wektorowej..."):
                        # Generowanie chmury słów na podstawie częstotliwości
                        wordcloud = WordCloud(
                            width=800, 
                            height=500, 
                            background_color='white', 
                            colormap='viridis', # paleta barw
                            max_words=100,
                        ).generate_from_frequencies(tech_counts)
                        
                        # Renderowanie na ekranie używając biblioteki matplotlib
                        fig, ax = plt.subplots(figsize=(10, 6))
                        ax.imshow(wordcloud, interpolation='bilinear')
                        ax.axis('off') # Ukrywamy osie X i Y
                        st.pyplot(fig)
                        
                with col2:
                    st.subheader("📊 Top 15 Stacku")
                    df_top = tech_counts.head(15).reset_index()
                    df_top.columns = ['Technologia', 'Liczba ofert']
                    st.dataframe(df_top, hide_index=True, use_container_width=True)
                    
                # 3. Zostawiamy wykres słupkowy
                st.markdown("---")
                st.subheader("📈 Wykres słupkowy (Top 10)")
                st.bar_chart(tech_counts.head(10))
            else:
                st.info("Brak danych po oczyszczeniu kolumny technologii.")
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
            user_contract = st.selectbox("Typ umowy", ["B2B", "UoP", "Inna"])

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
        
# --- Zakładka 5: System Rekomendacji NLP ---
with tab_nlp:
    st.header("🎯 Inteligentne Dopasowanie Ofert (NLP)")
    st.markdown("Algorytm przeanalizuje Twoje umiejętności i matematycznie dopasuje je do bazy ofert.")
    
    try:
        df_nlp = fetch_poland_data()
        
        if not df_nlp.empty:
            user_skills = st.text_area(
                "Wpisz swoje technologie i doświadczenie (np. 'Python, SQL, AWS, Docker, 3 lata doświadczenia w budowaniu rurociągów danych'):",
                height=100
            )
            
            if st.button("Znajdź idealne oferty", type="primary"):
                if len(user_skills) < 5:
                    st.warning("Wpisz więcej informacji, aby algorytm miał na czym pracować!")
                else:
                    with st.spinner('Wektoryzacja danych i obliczanie macierzy podobieństwa...'):
                        df_nlp['technologie'] = df_nlp['technologie'].fillna('')
                        df_nlp['title'] = df_nlp['title'].fillna('')
                        df_nlp['kategoria'] = df_nlp['kategoria'].fillna('')
                        
                        df_nlp['tekst_do_analizy'] = df_nlp['kategoria'] + " " + df_nlp['title'] + " " + df_nlp['technologie']
                        corpus = df_nlp['tekst_do_analizy'].tolist()
                        
                        vectorizer = TfidfVectorizer(stop_words='english')
                        tfidf_matrix = vectorizer.fit_transform(corpus)
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
                                    
                                    zarobki = "Brak widełek"
                                    if pd.notna(row['salary_min']) and pd.notna(row['salary_max']):
                                        zarobki = f"{int(row['salary_min'])} - {int(row['salary_max'])} {row['currency']}"
                                    st.write(f"**Zarobki:** {zarobki}")
                                    
                                    st.write(f"**Wymagane technologie:** {row['technologie']}")
                                    st.markdown(f"[🔗 Kliknij, aby aplikować]({row['url']})")
                            else:
                                if i == 0:
                                    st.info("Brak silnego dopasowania w bazie dla podanych umiejętności.")
                                break

    except Exception as e:
        st.error(f"Błąd modułu NLP: {e}")