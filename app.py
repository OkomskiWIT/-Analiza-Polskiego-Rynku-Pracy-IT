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

@st.cache_data(ttl=3600)
def fetch_global_data():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM job_offers;", engine)
    df = df.reset_index(drop=True)
    if 'Lp.' not in df.columns:
        df.insert(0, 'Lp.', range(1, len(df) + 1))
    if 'remote' in df.columns:
        df['remote'] = df['remote'].map({True: "Tak", False: "Nie"}).fillna("Brak")
    return df

@st.cache_data(ttl=3600)
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

@st.cache_data(ttl=3600)
def get_tech_counts(df):
    if 'technologie' not in df.columns:
        return pd.Series(dtype=int)
        
    tech_series = df['technologie'].dropna().astype(str).str.split(',').explode()
    tech_series = tech_series.str.strip().str.upper()
    tech_series = tech_series[(tech_series != '') & (tech_series != 'NAN') & (tech_series != 'NONE')]
    return tech_series.value_counts()

@st.cache_data(ttl=3600)
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
                lat = loc.get('lat')
                lon = loc.get('lon')
                
                if lat and lon:
                    lat, lon = float(lat), float(lon)
                    if not (49.0 <= lat <= 55.0 and 14.0 <= lon <= 25.0):
                        continue
                    
                    coord_key = (lat, lon)
                    ulica = loc.get('street', '')
                    miasto = loc.get('city', '')
                    adres = f"{ulica}, {miasto}" if ulica else miasto
                    
                    zarobki = "Brak widełek"
                    if pd.notna(getattr(row, 'salary_min', None)) and pd.notna(getattr(row, 'salary_max', None)):
                        zarobki = f"{int(row.salary_min)} - {int(row.salary_max)} {row.currency}"
                    
                    if coord_key not in grouped_offers:
                        grouped_offers[coord_key] = {
                            'adres': adres,
                            'firmy': set(),
                            'oferty_html': []
                        }
                    
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

st.set_page_config(page_title="Rynek Pracy IT", layout="wide")
st.title("Analityka Rynku Pracy IT")

with st.spinner("Ładowanie danych z bazy..."):
    df_global_main = fetch_global_data()
    df_pl_main = fetch_poland_data()
    ml_model, ml_columns = load_ml_model()

tab_pl, tab_global, tab_tech, tab_ai, tab_nlp = st.tabs([
    "Rynek Polski & Zarobki", 
    "Rynek Globalny", 
    "🔥 Top Technologie", 
    "🤖 Kalkulator ML",
    "🎯 Dopasuj Ofertę (NLP)"
])

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

        st.metric("Liczba ofert", len(df_global_main))
        st.dataframe(
            df_global_main[existing_cols], 
            column_config=column_config, 
            hide_index=True, 
            use_container_width=True
        )
    except Exception as e:
        st.error(f"Błąd ładowania danych globalnych: {e}")

with tab_pl:
    st.header("Zarobki i Analiza (Polska)")
    try:
        if not df_pl_main.empty:
            st.metric("Liczba dostepnych ofert (PL)", len(df_pl_main))
            
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
            existing_cols_pl = [col for col in display_columns_pl if col in df_pl_main.columns]

            st.dataframe(
                df_pl_main[existing_cols_pl], 
                column_config=column_config_pl, 
                hide_index=True, 
                use_container_width=True
            )
            
            st.markdown("---")
            st.subheader("📊 Analiza Kategorii")
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Liczba ofert w danej kategorii**")
                oferty_kategorie = df_pl_main['kategoria'].value_counts().reset_index()
                oferty_kategorie.columns = ['Kategoria', 'Liczba ofert']
                st.bar_chart(data=oferty_kategorie, x='Kategoria', y='Liczba ofert')
            with col2:
                st.write("**Średnia pensja w kategorii (PLN)**")
                srednia_kategorie = df_pl_main.dropna(subset=['salary_avg']).groupby('kategoria')['salary_avg'].mean().reset_index()
                st.bar_chart(data=srednia_kategorie, x='kategoria', y='salary_avg')

            st.markdown("---")
            st.subheader("🗺️ Interaktywna Mapa Ofert Pracy")
            if st.button("🗺️ Załaduj i pokaż mapę", type="primary"):
                with st.spinner("Przetwarzanie tysięcy koordynatów..."):
                    m, laczna_liczba_pinezek, bledy_log = build_interactive_map(df_pl_main)

                if laczna_liczba_pinezek > 0:
                    st.success(f"Sukces! Załadowano próbkę {laczna_liczba_pinezek} ofert na mapę.")
                    import streamlit.components.v1 as components
                    m.save("temp_map.html") 
                    with open("temp_map.html", "r", encoding="utf-8") as f:
                        html_data = f.read() 
                    components.html(html_data, height=650)
                else:
                    st.error("Krytyczny błąd: Wygenerowano 0 pinezek.")
    except Exception as e:
        st.error(f"Błąd ładowania danych z Polski: {e}")

with tab_tech:
    st.header("🔥 Analiza Technologii i Wymagań na Rynku")
    try:
        tech_counts = get_tech_counts(df_pl_main)
        if not tech_counts.empty:
            col1, col2 = st.columns([2, 1]) 
            with col1:
                st.subheader("☁️ Chmura pożądanych technologii")
                with st.spinner("Generowanie grafiki wektorowej..."):
                    wordcloud = WordCloud(
                        width=800, height=500, background_color='white', 
                        colormap='viridis', max_words=100
                    ).generate_from_frequencies(tech_counts)
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
                    'kategoria': [user_kategoria], 'location': [user_location], 'seniority': [user_seniority],
                    'remote': [user_remote], 'contract_type': [user_contract], 'tech_python': [1 if user_python else 0],
                    'tech_java': [1 if user_java else 0], 'tech_data_sql': [1 if user_data else 0],
                    'tech_cloud': [1 if user_cloud else 0], 'tech_frontend': [1 if user_frontend else 0]
                })

                input_encoded = pd.get_dummies(input_data)
                input_encoded = input_encoded.reindex(columns=ml_columns, fill_value=0)
                prediction = ml_model.predict(input_encoded)[0]

                st.success(f"Estymowane widełki: **{prediction:,.0f} PLN**")
                st.caption(f"MAE modelu XGBoost: ~4723 PLN.")

            st.markdown("---")
            st.subheader("🧠 Wyjaśnialne AI (Co wpływa na pensję?)")
            if hasattr(ml_model, 'feature_importances_'):
                importances = ml_model.feature_importances_
                df_importance = pd.DataFrame({'Cecha': ml_columns, 'Waga (%)': importances * 100})
                df_importance = df_importance.sort_values(by='Waga (%)', ascending=False).head(10)
                
                def format_label(col_name):
                    translations = {'seniority_': 'Poziom: ', 'kategoria_': 'Kategoria: ', 'location_': 'Lokalizacja: ', 'contract_type_': 'Umowa: ', 'tech_': 'Tech: '}
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
            st.error("Błąd: Nie udało się załadować pików modelu (salary_model.pkl / model_columns.pkl).")
    except Exception as e:
        st.error(f"Błąd analizy modelu: {e}")
        
with tab_nlp:
    st.header("🎯 Inteligentne Dopasowanie Ofert (NLP)")
    try:
        if not df_pl_main.empty:
            vectorizer, tfidf_matrix, df_nlp = prepare_nlp_matrix(df_pl_main)
            
            user_skills = st.text_area(
                "Wpisz swoje technologie i doświadczenie (np. 'Python, SQL, AWS, Docker'):",
                height=100
            )
            if st.button("Znajdź idealne oferty", type="primary"):
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