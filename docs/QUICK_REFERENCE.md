# CandleThrob Quick Reference

## 🚀 Docker Compose Commands

### Development Mode (Default)
```bash
# Start (uses candlethrob:dev with embedded wallet)
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down

# Rebuild development image
docker build -f Dockerfile.dev -t candlethrob:dev .
```

### Production Mode
```bash
# Start (uses candlethrob:latest with mounted wallet)
docker compose -f docker-compose.yml up -d

# Stop
docker compose -f docker-compose.yml down

# Rebuild production image
docker build -t candlethrob:latest .
```

## 🔧 Current Setup

| Mode | Image | Wallet | Config File |
|------|-------|--------|-------------|
| **Development** | `candlethrob:dev` | Embedded | `docker-compose.override.yml` |
| **Production** | `candlethrob:latest` | Mounted | `docker-compose.yml` |

## 🎯 Apache Airflow DAGs

| DAG | Image | Purpose |
|-----|-------|---------|
| `candlethrob_ingestion_dev` | `candlethrob:dev` | Development testing |
| `candlethrob_ingestion_prod` | `candlethrob:latest` | Production deployment |

## 🔐 Security

### Development
- ⚠️ Wallet embedded in image
- 🔧 DEBUG logging
- 🚀 Higher rate limits (10/min)

### Production  
- ✅ Wallet mounted read-only
- 📊 INFO logging
- 🛡️ Conservative rate limits (5/min)

## 🛠️ Troubleshooting

```bash
# Check if CandleThrob module loads
docker compose exec candlethrob python -c "import CandleThrob; print('✅ OK')"

# Test GCS connection
docker compose exec candlethrob python -c "from CandleThrob.utils.gcs import test_gcs_connection; print('✅ GCS OK' if test_gcs_connection('candlethrob-candata') else '❌ GCS Failed')"

# Check GCS credentials
ls -la ./gcs/

# View Airflow logs
docker compose logs airflow-webserver
docker compose logs airflow-scheduler

# Restart services
docker compose restart
```

## 📍 URLs

- **Airflow UI**: http://localhost:8080
- **Logs**: `./logs/`
- **Data**: `./data/`

## 🔄 Switching Modes

```bash
# Development → Production
docker compose down
docker compose -f docker-compose.yml up -d

# Production → Development  
docker compose -f docker-compose.yml down
docker compose up -d
```
