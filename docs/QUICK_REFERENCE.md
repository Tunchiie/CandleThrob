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

## 🎯 Kestra Flows

| Flow | Image | Purpose |
|------|-------|---------|
| `ingest_data_optimized.yml` | `candlethrob:dev` | Development testing |
| `ingest_data_production.yml` | `candlethrob:latest` | Production deployment |

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

# Test database connection
docker compose exec candlethrob python -c "from CandleThrob.utils.oracle_conn import OracleDB; OracleDB(); print('✅ DB OK')"

# Check wallet files
ls -la Wallet_candleThroblake/

# View Kestra logs
docker compose logs kestra

# Restart services
docker compose restart
```

## 📍 URLs

- **Kestra UI**: http://localhost:8080
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
