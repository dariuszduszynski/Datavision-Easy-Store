# DES Marker Worker

## Overview

The Marker Worker is a critical component in the DES ingestion pipeline. It prepares catalog entries for packing by:

1. Generating unique DES names (Snowflake-like IDs)
2. Computing hash values for shard routing
3. Determining target shard for each file
4. Updating catalog status to trigger packing

## Architecture

### Processing Flow

```
┌─────────────────┐
│  Source Catalog │
│  (unprocessed)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Marker Worker  │◄──── Rate Limiter
│                 │
│  1. SELECT      │
│  2. Generate ID │
│  3. Compute Hash│
│  4. Assign Shard│
│  5. UPDATE      │
└────────┬────────┘
         │
    ┌────┴────┐
    │ Success │ Failure
    ▼         ▼
┌─────────┐  ┌──────────┐
│ Marked  │  │ Retry    │
│ (ready) │  │ or DLQ   │
└─────────┘  └──────────┘
```

### Components

#### 1. AdvancedFileMarker
Main worker class with:
- Batch processing with configurable size
- Row-level locking (`FOR UPDATE SKIP LOCKED`)
- Exponential backoff retry logic
- Graceful shutdown handling

#### 2. TokenBucketRateLimiter
Protects source database from overload:
- Configurable rate (ops/second)
- Token bucket algorithm
- Async-friendly implementation

#### 3. HashStrategy
Pluggable hash computation:
- Default: SHA-256
- Extensible for custom strategies
- First N bytes determine shard

#### 4. Dead Letter Queue
Permanent failure tracking:
- Separate table for investigation
- Includes error messages
- Can be retried manually

## Configuration

### Basic Setup

```python
from des.marker.advanced_marker import AdvancedFileMarker
from des.marker.models import MarkerConfig

config = MarkerConfig(
    batch_size=100,
    max_age_days=1,
    max_retries=3,
    retry_backoff_base=2.0,
    rate_limit_per_second=50.0,
    enable_dead_letter_queue=True,
)

marker = AdvancedFileMarker(
    session_factory=session_factory,
    config=config,
)

await marker.run_forever(interval_seconds=5)
```

### Environment Variables

See main README for full list. Key variables:

```bash
# Database
DES_DB_URL="postgresql+asyncpg://user:pass@host/db"

# Processing
DES_MARKER_BATCH_SIZE=200
DES_MARKER_MAX_AGE_DAYS=1
DES_MARKER_RATE_LIMIT=100.0

# Naming
DES_NODE_ID=1
DES_NAME_PREFIX="DES"
DES_SHARD_BITS=8
```

## Deployment

### Docker Compose

```bash
# Start marker worker
docker-compose up marker

# Scale to 3 workers
docker-compose up --scale marker=3

# View logs
docker-compose logs -f marker
```

### Kubernetes

```bash
# Deploy
kubectl apply -f k8s/marker-deployment.yaml

# Enable autoscaling
kubectl apply -f k8s/marker-hpa.yaml

# Check status
kubectl get pods -l app=des-marker
kubectl logs -l app=des-marker -f

# Scale manually
kubectl scale deployment/des-marker --replicas=5
```

## Monitoring

### Prometheus Metrics

Key metrics to monitor:

#### Throughput
```promql
# Entries marked per second
rate(des_marker_entries_marked_total{status="success"}[1m])

# Batch duration
histogram_quantile(0.95, rate(des_marker_batch_duration_seconds_bucket[5m]))
```

#### Errors
```promql
# Error rate
rate(des_marker_errors_total[5m])

# Errors by type
sum by (error_type) (rate(des_marker_errors_total[5m]))

# Retry rate
rate(des_marker_retries_total[5m])
```

#### Resource Usage
```promql
# CPU usage
rate(container_cpu_usage_seconds_total{pod=~"des-marker.*"}[5m])

# Memory usage
container_memory_working_set_bytes{pod=~"des-marker.*"}
```

### Grafana Dashboard

Import dashboard from `config/grafana/dashboards/marker.json`:

**Panels:**
1. Processing Rate (entries/sec)
2. Batch Duration (p50, p95, p99)
3. Error Rate by Type
4. Retry Attempts
5. DLQ Size
6. Resource Usage (CPU, Memory)
7. Pod Count (current/desired)

### Alerting

Recommended alerts:

```yaml
groups:
  - name: des-marker
    interval: 30s
    rules:
      - alert: MarkerHighErrorRate
        expr: rate(des_marker_errors_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate in marker worker"

      - alert: MarkerStalled
        expr: rate(des_marker_entries_marked_total[5m]) == 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Marker worker not processing entries"

      - alert: MarkerDLQGrowing
        expr: increase(des_marker_dlq_entries_total[1h]) > 100
        labels:
          severity: warning
        annotations:
          summary: "Dead letter queue growing rapidly"
```

## Troubleshooting

### Common Issues

#### 1. Marker Not Processing
**Symptoms:** `des_marker_entries_marked_total` flat

**Checks:**
```bash
# Check database connection
kubectl exec -it deployment/des-marker -- psql $DES_DB_URL -c "SELECT 1"

# Check for unprocessed entries
kubectl exec -it deployment/des-marker -- psql $DES_DB_URL -c \
  "SELECT COUNT(*) FROM des_source_catalog WHERE des_status IS NULL"

# Check logs for errors
kubectl logs -l app=des-marker --tail=100
```

#### 2. High Error Rate
**Symptoms:** `des_marker_errors_total` increasing

**Investigation:**
```bash
# Check error types
kubectl logs -l app=des-marker | grep error_type

# Check DLQ
psql $DES_DB_URL -c "SELECT error_message, COUNT(*) FROM des_marker_dlq 
  WHERE NOT resolved GROUP BY error_message ORDER BY COUNT(*) DESC"
```

#### 3. Database Overload
**Symptoms:** Slow batch processing, high DB CPU

**Solutions:**
- Reduce `DES_MARKER_BATCH_SIZE`
- Enable rate limiting: `DES_MARKER_RATE_LIMIT=50.0`
- Scale horizontally (add more pods)
- Add database indices (see init_db.sql)

#### 4. Memory Issues
**Symptoms:** OOMKilled pods

**Solutions:**
- Reduce batch size
- Increase memory limits in K8s deployment
- Check for memory leaks (heap dump)

### Debug Mode

Enable detailed logging:
```bash
kubectl set env deployment/des-marker LOG_LEVEL=DEBUG
```

### Manual DLQ Processing

Retry failed entries:
```sql
-- View DLQ entries
SELECT * FROM des_marker_dlq WHERE NOT resolved ORDER BY created_at DESC LIMIT 10;

-- Mark for retry
UPDATE des_source_catalog
SET des_status = 'retry', retry_count = 0
WHERE id IN (
    SELECT catalog_entry_id FROM des_marker_dlq WHERE NOT resolved
);

-- Mark DLQ as resolved
UPDATE des_marker_dlq SET resolved = TRUE, resolved_at = NOW()
WHERE catalog_entry_id IN (...);
```

## Performance Tuning

### Batch Size Selection

Rule of thumb:
- **Small files (<1KB)**: batch_size=200-500
- **Medium files (1-100KB)**: batch_size=100-200
- **Large files (>100KB)**: batch_size=50-100

Monitor `des_marker_batch_duration_seconds` and adjust.

### Rate Limiting

Calculate based on database capacity:
```python
# Example: Database can handle 5000 queries/min
queries_per_batch = 3  # SELECT, UPDATE, possible retry
batch_per_second = 5000 / 60 / 3  # ~27 batches/sec
rate_limit = batch_per_second * batch_size  # ~2700 ops/sec
```

### Horizontal Scaling

Determine optimal replica count:
```python
# Example: 1M entries to process daily
entries_per_day = 1_000_000
entries_per_second = entries_per_day / 86400  # ~12 entries/sec

# With batch_size=100, processing_time=2sec
throughput_per_pod = batch_size / processing_time  # 50 entries/sec/pod

replicas = entries_per_second / throughput_per_pod  # ~1 pod
# Add 50% buffer for spikes: 2 pods
# Add HA requirement: 3 pods minimum
```

## Best Practices

1. Always enable DLQ in production.
2. Set rate limits to protect your database.
3. Monitor metrics and set alerts.
4. Test failover by killing pods and verifying recovery.
5. Review DLQ contents regularly to spot patterns.
6. Keep database indices current for marker queries.
7. Use graceful shutdown and sane resource limits.

## API Reference

See inline documentation in:
- `src/des/marker/advanced_marker.py`
- `src/des/marker/models.py`
- `src/des/marker/rate_limiter.py`
