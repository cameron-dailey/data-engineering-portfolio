# Dockerized Steel City Ops Analytics

### Quickstart
1. **Build and run all containers**
```bash
docker-compose up --build
```

2. **Access the Streamlit dashboard**
```
http://localhost:8501
```

3. **PostgreSQL DB access**
```
Host: localhost
Port: 5432
User: steelcity
Password: steelcity
DB: steelcity
```

4. **Stop containers**
```bash
docker-compose down
```

This automatically:
- Spins up PostgreSQL (loads schema automatically)
- Runs all ETL + transformations
- Trains maintenance model
- Launches Streamlit dashboard