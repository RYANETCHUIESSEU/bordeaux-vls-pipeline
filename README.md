#  Bordeaux VLS Tracker - Pipeline ETL & Dashboard

Ce projet est une solution complète d'analyse de données pour le réseau de vélos en libre-service de Bordeaux. Il récupère les données en temps réel via l'API CityBikes, les stocke de manière incrémentielle et visualise l'activité des stations.

## Fonctionnalités
- **Extraction (ETL)** : Script Python automatisé pour récupérer la disponibilité des vélos.
- **Stockage** : Base de données SQLite pour conserver l'historique des stations.
- **Transformation** : Calcul des flux (départs/arrivées) via la bibliothèque Pandas.
- **Visualisation** : Interface interactive développée avec Streamlit.

## Installation & Utilisation
1. Cloner le projet : `git clone [LIEN_DE_TON_REPO]`
2. Installer les dépendances : `pip install -r requirements.txt`
3. Lancer l'extraction : `python extract.py`
4. Lancer le dashboard : `streamlit run app.py`

##  Stack Technique
- Python (Requests, Pandas, SQLite)
- Streamlit (Dashboarding)
