FROM ghcr.io/dbt-labs/dbt-snowflake:1.5.0

WORKDIR /usr/app

# Copier les fichiers du projet
COPY . .

# Installer les dépendances dbt
RUN dbt deps