# Reddit Sentiment Streaming Pipeline

Ce projet met en place une **pipeline de streaming** qui récupère des posts Reddit, les envoie via Kafka, les consomme avec Spark, effectue un nettoyage et une analyse de sentiment, puis stocke les résultats dans Cassandra. Le tout est orchestré via Airflow et conteneurisé avec Docker.

---

##  Architecture

1. **Producer** :
   - Script `producer_faker.py` utilisant PRAW pour récupérer des posts Reddit par mots-clés.
   - Nettoyage des URLs et génération d'UUID pour chaque post.
   - Envoi des posts vers Kafka.

2. **Consumer Spark** :
   - Script `spark_consumer.py` qui lit les messages Kafka.
   - Nettoyage du texte, tokenisation, lemmatisation et suppression des mots vides.
   - Analyse de sentiment via `VaderSentiment`.
   - Écriture des résultats dans Cassandra.

3. **Orchestration Airflow** :
   - DAG `kafka_spark_streaming_orchestration` qui démarre le producer et le consumer Spark.
   - Vérification préalable que Kafka est en fonctionnement.

4. **Docker & Docker Compose** :
   - Conteneurisation des services (Kafka, Zookeeper, schema-registry, control-center, Cassandra, Airflow).
   - Scripts d'entrypoint pour faciliter le démarrage d'Airflow.

---

##  Prérequis

- Docker & Docker Compose
- Python 3.10+
- Kafka et Zookeeper
- Cassandra
- Airflow

### Packages Python

- `praw`
- `python-dotenv`
- `kafka-python`
- `pyspark`
- `vaderSentiment`
- `nltk`
- `uuid`

Installer avec :
```bash
pip install -r requirements.txt
```

---

##  Configuration

1. **Variables d'environnement** :
   cp env.example .env

   ```env
   CLIENT_ID="ton client id"
   CLIENT_SECRET="ton secret"
   ```

2. **Docker Compose** :
   Pour lancer tous les services :
   ```bash
   docker compose up -d
   ```

   Pour lancer des services individuellement (ex. Airflow) :
   ```bash
   docker compose up -d postgres
   docker compose up -d webserver
   docker compose up -d scheduler
   ```

    Parfois, il faut attendre que certains serveurs soient opérationnels avant de lancer Airflow ou Kafka.

3. **Permissions Entrypoint** :
   ```bash
   chmod +x script/entrypoint.sh
   ```

4. **Créer les topics Kafka avant de lancer le producer** :
   ```bash
   docker exec -it broker kafka-topics --bootstrap-server localhost:29092 \
     --create --topic reddit_stock_comments --partitions 1 --replication-factor 1
   ```

5. **Arrêt et nettoyage** :
   ```bash
   docker compose down -v
   ```

---

## Utilisation

### Lancer le DAG Airflow

1. Accéder à l'interface Web d'Airflow (`http://localhost:8080`)
2. Déclencher manuellement le DAG `kafka_spark_streaming_orchestration`

### Lancer directement les scripts (hors Airflow)

```bash
python producer_faker.py
python spark_consumer.py
```

---

##  Notes importantes

* Tout fork doit inclure les variables `.env`.
* Les volumes Docker doivent être définis pour persister les données Cassandra et les logs Airflow.
* Assurez-vous que Spark, Kafka et Cassandra sont opérationnels avant de lancer les scripts.
* Vérifiez les logs Airflow :
  ```bash
  docker logs -f data_eng_project-webserver-1
  ```

---

##  Astuces

* Pour vérifier le fonctionnement de Kafka :
  ```bash
  docker exec -it broker kafka-topics --list --bootstrap-server localhost:29092
  ```

* Pour simuler du flux de données, modifiez l'intervalle dans `stream_to_kafka()`.