# Mac OS Docker Stuff

Copy these files into your directory replacing the existing dockerfile.airflow AND docker-compose.yml.

## 1. Stop and remove existing containers

```docker compose -f docker-compose.macos.yml down```

## 2. Rebuild images (forces rebuild of Airflow image with correct JAVA_HOME)

```docker compose -f docker-compose.macos.yml build --no-cache```

## 3. Start fresh

```docker compose -f docker-compose.macos.yml up -d```
