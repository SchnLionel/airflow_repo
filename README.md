# Airflow Datascientest Project

Projet d'orchestration de données basé sur Apache Airflow 2.8.1.

## Installation et Lancement

1. **Configurer les permissions et l'environnement** :
   ```bash
   mkdir -p ./dags ./logs ./plugins
   chmod -R 777 logs dags plugins
   echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
   ```

2. **Initialiser la base de données** :
   ```bash
   docker compose up airflow-init
   ```

3. **Lancer les conteneurs** :
   ```bash
   docker compose up -d
   ```

## Accès
L'interface web est accessible sur le port **8080**.
Utilisateur par défaut : `airflow` / `airflow`.
