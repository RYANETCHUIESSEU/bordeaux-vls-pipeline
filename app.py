import streamlit as st
import pandas as pd
import sqlite3
import requests
import time
from datetime import datetime

# =========================================================================
# ⚙️ CONFIGURATION ET DÉFINITIONS GLOBALES
# =========================================================================

# Variables de connexion et de contrôle
API_URL = "https://api.citybik.es/v2/networks/v3-bordeaux"
DB_NAME = "bordeaux_bikes.db"
POLL_INTERVAL_SECONDS = 300  # 5 minutes pour la mise en cache/actualisation

# Définitions SQL pour la création et l'insertion
SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS station_status (
    snapshot_time TEXT, station_id TEXT, name TEXT, 
    num_bikes_available INTEGER, num_docks_available INTEGER, 
    latitude REAL, longitude REAL
);
"""
SQL_INSERT = "INSERT INTO station_status VALUES (?, ?, ?, ?, ?, ?, ?)"

# Requête SQL pour l'Analyse de Classement (Utilisation Moyenne)
SQL_RANKING_QUERY = """
SELECT
    station_id,
    name,
    COUNT(*) AS snapshots_count,
    SUM(num_bikes_available) AS total_bikes,
    AVG(num_bikes_available) AS avg_bikes,
    ROUND(AVG(CAST(num_bikes_available AS REAL) / (num_bikes_available + num_docks_available)) * 100, 2) AS avg_utilization_percent
FROM
    station_status
GROUP BY
    station_id, name
ORDER BY
    avg_bikes DESC
LIMIT 10;
"""

# =========================================================================
# 💾 LOGIQUE DU PIPELINE E-T-L
# =========================================================================

def load_data_to_sqlite(stations_data, current_time_str):
    """Gère la connexion, la création de table et l'insertion des données."""
    conn = None
    if not stations_data:
        # print("Avertissement: Aucune donnée à insérer.") # Supprimé pour Streamlit
        return

    try:
        conn = sqlite3.connect("bordeaux_bikes.db")
        cursor = conn.cursor()
        cursor.execute(SQL_CREATE_TABLE)

        rows_to_insert = []
        for station in stations_data:
            # Transformation T : Structuration des données
            row = (
                current_time_str,
                station.get('id'),
                station.get('name'),
                station.get('free_bikes'),
                station.get('empty_slots'),
                station.get('latitude'),
                station.get('longitude')
            )
            rows_to_insert.append(row)

        cursor.executemany(SQL_INSERT, rows_to_insert)
        conn.commit()
        # st.success(f"✅ Chargement réussi : {len(rows_to_insert)} lignes insérées.") # Utilisé dans Streamlit
        return True # Succès

    except sqlite3.Error as e:
        st.error(f"❌ Erreur SQLite lors du chargement: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

def run_ingestion_cycle():
    """Exécute un cycle complet d'Extraction, Transformation et Chargement."""
    
    # 1. Extraction (E)
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Lève une erreur pour les statuts 4xx/5xx

        # 2. Transformation (T)
        data = response.json()
        stations_data = data.get('network', {}).get('stations', [])
        current_time_str = datetime.now().isoformat()
        
        # 3. Chargement (L)
        return load_data_to_sqlite(stations_data, current_time_str)
        
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Erreur lors de la requête API ou de la connexion : {e}")
        return False
    except Exception as e:
        st.error(f"❌ Erreur inattendue dans le cycle d'ingestion : {e}")
        return False

# =========================================================================
# 📊 FONCTIONS D'AFFICHAGE ET D'ANALYSE (STREAMLIT)
# =========================================================================

@st.cache_data(ttl=POLL_INTERVAL_SECONDS)
def get_station_data_for_display():
    """Récupère l'état le plus récent de toutes les stations pour l'affichage."""
    conn = None
    try:
        conn = sqlite3.connect("bordeaux_bikes.db")
        # Requête pour obtenir le dernier état enregistré
        query = """
        SELECT * FROM station_status 
        WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM station_status);
        """
        df = pd.read_sql_query(query, conn)
        return df
    except sqlite3.Error as e:
        st.error(f"Erreur de base de données lors de la récupération des données temps réel : {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600) # Analyse mise en cache pour 1 heure
def get_ranking_analysis():
    """Exécute la requête d'analyse (Utilisation Moyenne) et retourne un DataFrame."""
    conn = None
    try:
        conn = sqlite3.connect("bordeaux_bikes.db")
        df = pd.read_sql_query(SQL_RANKING_QUERY, conn)
        
        # Mise en forme pour l'affichage Streamlit
        if not df.empty:
            df['avg_utilization_percent'] = df['avg_utilization_percent'].round(2).astype(str) + '%'
            df = df.rename(columns={
                'snapshots_count': 'Snapshots',
                'avg_bikes': 'Vélos Moy.',
                'avg_utilization_percent': 'Utilisation %'
            })
        return df
    except sqlite3.Error as e:
        st.error(f"[ERREUR] Erreur lors de l'exécution de l'analyse SQL : {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"[ERREUR] Erreur inattendue lors de l'analyse : {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()


# =========================================================================
# 🌐 APPLICATION STREAMLIT PRINCIPALE
# =========================================================================

def main():
    st.set_page_config(layout="wide")
    st.title("🚲 Surveillance du Réseau VCUB (Bordeaux) - Pipeline E-T-L-A")
    st.caption("Données récupérées de l'API CityBikes, stockées dans SQLite, et visualisées via Streamlit.")
    
    # --- Contrôle d'Ingestion ---
    col_button, col_time = st.columns([1, 2])
    
    if col_button.button("🔄 Lancer l'Ingestion E-T-L Maintenant"):
        with st.spinner('Extraction, Transformation et Chargement des données en cours...'):
            run_ingestion_cycle()
        st.experimental_rerun() # Rafraîchir toute l'interface après l'ingestion réussie

    st.markdown("---")

    # --- 1. État Temps Réel et Visualisation ---
    st.header("1. État des Stations en Temps Quasi-Réel 🟢")
    
    current_df = get_station_data_for_display()
    
    if not current_df.empty:
        # Affichage du dernier instantané
        latest_time = current_df['snapshot_time'].max()
        col_time.info(f"Dernier instantané capturé : **{latest_time}** | Cache actualisé toutes les {POLL_INTERVAL_SECONDS/60:.0f} min.")
        
        # Métriques agrégées
        col1, col2, col3 = st.columns(3)
        col1.metric(label="Total Stations Actives", value=len(current_df))
        col2.metric(label="Total Vélos Disponibles", value=current_df['num_bikes_available'].sum())
        col3.metric(label="Total Quais Vides", value=current_df['num_docks_available'].sum())

        st.subheader("Visualisation Géographique : Vélos Disponibles")
        
        # Préparation des données pour la carte
        map_df = current_df.rename(columns={'latitude': 'lat', 'longitude': 'lon'})
        
        # Affichage de la carte
        st.map(map_df, latitude='lat', longitude='lon', size='num_bikes_available', color='#FF4B4B') 
        
        st.subheader("Détail du Dernier Instantané")
        st.dataframe(current_df[['name', 'num_bikes_available', 'num_docks_available', 'snapshot_time']], 
                 width='stretch', hide_index=True)
    else:
        st.warning("Aucune donnée n'a encore été chargée dans la base de données. Veuillez lancer l'ingestion E-T-L.")
    
    st.markdown("---")

    # --- 2. Analyse de l'Activité (Classement) ---
    st.header("2. Analyse de l'Activité Historique 📈")
    st.caption("Classement des stations les plus sollicitées basé sur la 'Rotation Totale' (mouvements) depuis le début de la collecte.")
    
    ranking_df = get_ranking_analysis()
    
    if not ranking_df.empty:
        st.subheader("Top 10 Stations VCUB")
        
        # Affichage du tableau de classement
        st.dataframe(ranking_df[['name', 'Vélos Moy.', 'Utilisation %', 'Snapshots']], 
                 width='stretch', hide_index=True)
                     
        # Visualisation de l'utilisation moyenne
        st.subheader("Graphique d'Utilisation Moyenne")
        chart_df = ranking_df.copy()
        chart_df['Utilisation %'] = chart_df['Utilisation %'].str.rstrip('%').astype(float)
        st.bar_chart(chart_df.set_index('name')[['Utilisation %']])

    else:
        st.warning("L'analyse nécessite au moins deux cycles d'ingestion pour calculer la rotation des vélos. Veuillez collecter des données pendant un certain temps.")

if __name__ == "__main__":
    main()