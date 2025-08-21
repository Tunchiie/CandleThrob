# 📋 Docker Logs Guide for CandleThrob

This guide provides comprehensive instructions for checking logs in your CandleThrob Docker environment, including container logs, application logs, and troubleshooting techniques.

## 🔍 **Quick Reference Commands**

### **Check Running Containers**
```bash
# List all running containers
docker ps

# List all containers (including stopped)
docker ps -a

# Filter for CandleThrob containers only
docker ps | grep candlethrob
```

### **Basic Log Commands**
```bash
# View logs for a specific container
docker logs <container_name_or_id>

# Follow logs in real-time
docker logs -f <container_name_or_id>

# Show last 50 lines
docker logs --tail 50 <container_name_or_id>

# Show logs with timestamps
docker logs -t <container_name_or_id>
```

## 🐳 **Container-Specific Log Commands**

### **CandleThrob Application Logs**
```bash
# Find your CandleThrob container
docker ps | grep candlethrob

# View latest logs (replace with your container name)
docker logs --tail 100 -f <candlethrob_container_name>

# Example with actual container name
docker logs --tail 100 -f elated_jepsen
```

### **Apache Airflow Workflow Logs**
```bash
# View Airflow orchestration logs
docker logs --tail 50 -f airflow-webserver
docker logs --tail 50 -f airflow-scheduler

# Check Airflow startup logs
docker logs airflow-webserver | head -20
docker logs airflow-scheduler | head -20

# Monitor Airflow workflow execution
docker logs -f airflow-scheduler | grep -E "(RUNNING|SUCCESS|FAILED|ERROR)"
```

## 📁 **Application Log Files Inside Containers**

### **Access CandleThrob Internal Logs**
```bash
# Enter the container
docker exec -it <candlethrob_container> bash

# Navigate to log directory
cd /app/logs

# List available log files
ls -la *.log

# View latest ingestion logs
tail -f candlethrob_ingestion_*.log

# View database logs
tail -f candlethrob_database_*.log

# View audit logs
tail -f candlethrob_audit_*.log
```

### **Common Log File Patterns**
```bash
# Ingestion logs (timestamped)
/app/logs/candlethrob_ingestion_YYYYMMDD_HHMMSS.log

# Database operation logs
/app/logs/candlethrob_database_YYYYMMDD_HHMMSS.log

# Audit trail logs
/app/logs/candlethrob_audit_YYYYMMDD_HHMMSS.log
```

## 🔧 **Advanced Log Monitoring**

### **Real-Time Multi-Container Monitoring**
```bash
# Monitor multiple containers simultaneously
docker logs -f airflow-scheduler &
docker logs -f airflow-webserver &
docker logs -f <candlethrob_container> &

# Use docker-compose logs (if using compose)
docker-compose logs -f

# Monitor specific services
docker-compose logs -f candlethrob airflow-scheduler airflow-webserver
```

### **Filtered Log Viewing**
```bash
# Filter for errors only
docker logs <container> 2>&1 | grep -i error

# Filter for specific operations
docker logs <container> | grep -i "ingestion\|macro\|transform"

# Filter by timestamp (last hour)
docker logs --since 1h <container>

# Filter by date range
docker logs --since "2025-07-15T10:00:00" --until "2025-07-15T12:00:00" <container>
```

## 🚨 **Troubleshooting Specific Issues**

### **Check Ingestion Flow Status**
```bash
# Check if ingestion completed
docker exec <candlethrob_container> ls -la /app/data/

# View completion flag
docker exec <candlethrob_container> cat /app/data/batch_cycle_complete.flag

# Check ingestion results
docker exec <candlethrob_container> head -20 /app/data/ingestion_results.json
```

### **Database Connection Issues**
```bash
# Check Oracle client logs
docker logs <candlethrob_container> | grep -i "oracle\|tns\|database"

# Check wallet files
docker exec <candlethrob_container> ls -la /opt/oracle/instantclient_23_8/network/admin/

# Test GCS connectivity
docker exec <candlethrob_container> python -c "from CandleThrob.utils.gcs import test_gcs_connection; test_gcs_connection('candlethrob-candata')"
```

### **Workflow Orchestration Issues**
```bash
# Check Airflow execution status
curl -s http://localhost:8080/api/v1/dags/candlethrob_ingestion/dagRuns

# View Airflow scheduler logs
docker logs airflow-scheduler | grep -E "(execution|dag|task)"

# Check for failed workflows
docker logs airflow-scheduler | grep -i "failed\|error\|exception"
```

## 📊 **Log Analysis Commands**

### **Performance Monitoring**
```bash
# Check memory usage in logs
docker logs <container> | grep -i "memory\|mb\|gb"

# Check processing times
docker logs <container> | grep -i "duration\|seconds\|completed"

# Check success/failure rates
docker logs <container> | grep -E "successful|failed" | tail -20
```

### **Data Quality Monitoring**
```bash
# Check data quality scores
docker logs <container> | grep -i "quality.*score"

# Check validation results
docker logs <container> | grep -i "validation"

# Check batch processing results
docker logs <container> | grep -i "batch.*completed"
```

## 🔄 **Automated Log Monitoring Scripts**

### **Create a Log Monitoring Script**
```bash
# Create monitoring script
cat > monitor_logs.sh << 'EOF'
#!/bin/bash

echo "=== CandleThrob Log Monitor ==="
echo "Timestamp: $(date)"
echo

# Find CandleThrob container
CONTAINER=$(docker ps | grep candlethrob | awk '{print $1}' | head -1)

if [ -z "$CONTAINER" ]; then
    echo "❌ No CandleThrob container found"
    exit 1
fi

echo "📦 Container: $CONTAINER"
echo

# Check latest logs
echo "🔍 Latest Application Logs:"
docker logs --tail 10 $CONTAINER

echo
echo "📁 Internal Log Files:"
docker exec $CONTAINER ls -la /app/logs/ | tail -5

echo
echo "🏁 Completion Status:"
if docker exec $CONTAINER test -f /app/data/batch_cycle_complete.flag; then
    echo "✅ Ingestion completed at: $(docker exec $CONTAINER cat /app/data/batch_cycle_complete.flag)"
else
    echo "⏳ Ingestion still running or not started"
fi
EOF

chmod +x monitor_logs.sh
```

### **Run the Monitor**
```bash
# Execute the monitoring script
./monitor_logs.sh

# Run continuously (every 30 seconds)
watch -n 30 ./monitor_logs.sh
```

## 📝 **Log Rotation and Cleanup**

### **Clean Up Old Logs**
```bash
# Remove old container logs (be careful!)
docker system prune --volumes

# Clean up application logs inside container
docker exec <container> find /app/logs -name "*.log" -mtime +7 -delete

# Rotate logs manually
docker exec <container> logrotate /etc/logrotate.conf
```

## 🎯 **Quick Troubleshooting Checklist**

1. **Container Status**: `docker ps -a`
2. **Recent Logs**: `docker logs --tail 50 <container>`
3. **Error Messages**: `docker logs <container> | grep -i error`
4. **Completion Status**: Check `/app/data/batch_cycle_complete.flag`
5. **Resource Usage**: `docker stats <container>`
6. **Internal Logs**: `docker exec <container> ls -la /app/logs/`

## 🔗 **Related Commands**

```bash
# Container resource usage
docker stats

# Container inspection
docker inspect <container>

# Container processes
docker exec <container> ps aux

# Container disk usage
docker exec <container> df -h
```

---

**💡 Pro Tip**: Always use container names instead of IDs for easier log management. You can set custom names when running containers with `docker run --name my-container-name`.

**⚠️ Warning**: Be careful when cleaning up logs in production environments. Always backup important logs before deletion.
