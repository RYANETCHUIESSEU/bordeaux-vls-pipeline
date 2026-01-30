"""Synthèse et Exercice Pratique
Points Clés à Retenir
    APIs comme Passerelles de Données : Les APIs sont des sources fondamentales de données structurées et en temps réel, essentielles pour l'enrichissement, la synchronisation et l'analyse en direct des données.
    Stratégie ETL vs ELT : Le choix définit le lieu de la transformation : ETL (avant le stockage) pour le contrôle qualité, ou ELT (après le chargement) pour exploiter la puissance de calcul de l'entrepôt de données.
    Automatisation et Surveillance : Automatiser l'ingestion de données (avec Airflow ou cron) et surveiller de près les métriques d'exécution, la qualité des données et les taux d'erreur pour maintenir la fiabilité du pipeline.
    Sécurité et Gestion des Erreurs : Assurer la sécurité des identifiants (variables d'environnement, magasins de secrets) et intégrer une logique de réessai (retry logic), une limitation de débit (rate limiting) et une journalisation complète pour garantir la résilience de la production.
Exercice Pratique : Surveillance des Stations de Vélos de Bordeaux
L'objectif de cet exercice est de consommer l'API VCUB pour surveiller la disponibilité des vélos en temps réel et analyser les modèles d'utilisation du réseau de vélos en libre-service de Bordeaux, combinant l'extraction, la transformation et l'analyse.
Étapes de l'exercice :
    Récupérer les Données de Station en Temps Réel : Se connecter à l'API VCUB pour obtenir les informations actuelles (vélos disponibles, quais vides et emplacements géographiques).
    Suivre l'Activité au Fil du Temps : Interroger l'API à intervalles réguliers (ex. toutes les 5 minutes) pour observer les changements d'activité, identifier les stations occupées/calmes et les périodes de pointe.
    Classer les Stations les Plus Actives : Calculer des métriques (vélos déplacés, taux de rotation, pourcentage d'utilisation) pour établir un classement des stations les plus sollicitées du réseau.
Note : SQLite sera utilisé comme solution de stockage pour cet exercice."""
import requests
import sqlite3
from datetime import datetime
import time
import sys

# --- CONFIGURATION ---
API_URL = "https://api.citybik.es/v2/networks/v3-bordeaux"
DB_NAME = "bordeaux_bikes.db"
POLL_INTERVAL_SECONDS = 300  # Intervalle de 5 minutes (300 secondes)

# Définitions SQL
SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS station_status (
    snapshot_time TEXT, station_id TEXT, name TEXT, 
    num_bikes_available INTEGER, num_docks_available INTEGER, 
    latitude REAL, longitude REAL
);
"""
SQL_INSERT = "INSERT INTO station_status VALUES (?, ?, ?, ?, ?, ?, ?)"

# ===============================================
# LOGIQUE DE CHARGEMENT (L)
# ===============================================

def load_data_to_sqlite(stations_data, current_time_str):
    """Gère la connexion, la création de table et l'insertion des données."""
    conn = None
    if not stations_data:
        print("Avertissement: Aucune donnée à insérer.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
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
        print(f"✅ Chargement réussi : {len(rows_to_insert)} lignes insérées.")

    except sqlite3.Error as e:
        print(f"❌ Erreur SQLite : {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# ===============================================
# FONCTION DU CYCLE D'INGESTION (E-T-L Réunies)
# ===============================================

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
        load_data_to_sqlite(stations_data, current_time_str)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête API ou de la connexion : {e}")
    except Exception as e:
        print(f"❌ Erreur inattendue dans le cycle : {e}")

# ===============================================
# LOGIQUE D'ANALYSE (A)
# ===============================================

def analyze_and_rank_stations():
    """
    Exécute une analyse complète des stations et affiche le classement
    des 10 stations les plus actives selon la rotation des vélos.
    """
    
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
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        print("\n" + "="*70)
        print("[ANALYSE] Top 10 Stations par Utilisation Moyenne")
        print("="*70)
        
        cursor.execute(SQL_RANKING_QUERY)
        results = cursor.fetchall()
        
        if not results:
            print("[ATTENTION] Aucune donnée disponible pour l'analyse.")
            return
        
        print(f"{'Station ID':<20} {'Nom':<25} {'Snapshots':<12} {'Util. %':<10}")
        print("-" * 70)
        
        for row in results:
            station_id, name, count, total, avg, util = row
            util_str = f"{util:.2f}%" if util else "0.00%"
            print(f"{station_id:<20} {str(name)[:24]:<25} {count:<12} {util_str:<10}")
        
        print("="*70 + "\n")
        
    except sqlite3.Error as e:
        print(f"[ERREUR] Erreur SQL lors de l'analyse : {e}")
    except Exception as e:
        print(f"[ERREUR] Erreur inattendue lors de l'analyse : {e}")
    finally:
        if conn:
            conn.close()


# ===============================================
# BOUCLE PRINCIPALE (Automatisation)
# ===============================================

if __name__ == "__main__":
    
    print(f"[DEBUT] Démarrage du moniteur de stations de Bordeaux. Collecte toutes les {POLL_INTERVAL_SECONDS} secondes.")
    
    try:
        # BOUCLE INFINIE POUR L'AUTOMATISATION
        while True:
            timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp_start}] --- Début du cycle d'ingestion ---")
            
            # Exécute la tâche E-T-L
            run_ingestion_cycle()
            
            # Exécute l'analyse après chaque cycle (c'est le monitoring !)
            analyze_and_rank_stations()
            
            timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp_end}] --- Cycle terminé. Mise en veille pour {POLL_INTERVAL_SECONDS} secondes ---")
            
            # Pause le script pour l'intervalle spécifié
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n\n Arrêt de la collecte des données par l'utilisateur.")