FROM fishtownanalytics/dbt-snowflake:1.5.0

WORKDIR /usr/app

# Copier les fichiers du projet
COPY . .

# Installer les dépendances dbt
RUN dbt deps

# Installer les dépendances supplémentaires si besoin
RUN pip install --no-cache-dir \
    dbt-snowflake==1.5.0

# Définir le point d'entrée
ENTRYPOINT ["dbt"]