# Utiliser une image Python officielle
FROM python:3.12

# Définir le dossier de travail à l’intérieur du conteneur
WORKDIR /Data_eng-Project
# Copier les fichiers requirements et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout ton code dans le conteneur
COPY . .

# Lancer le programme principal quand le conteneur démarre
CMD ["python", "Data_eng_Project/main.py"]
