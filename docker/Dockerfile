FROM fishtownanalytics/dbt:1.0.0-snowflake

WORKDIR /usr/app

# Installer les dépendances système si nécessaire
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copier les fichiers du projet
COPY . .

# Installer les dépendances dbt
RUN dbt deps

ENTRYPOINT ["dbt"]
CMD ["--help"]