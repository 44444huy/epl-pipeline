# EPL Realtime Data Pipeline

A production-grade data pipeline for English Premier League real-time data using modern data engineering tools.

## Architecture
```
Football API → Smart Producer → Kafka → Kafka Connect → AWS S3
                                                          ↓
                                              Spark → dbt → Redshift → Dashboard
```

Airflow orchestrates the entire pipeline.

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 3.6 | Message broker |
| Apache Airflow | 2.8.1 | Orchestration |
| AWS S3 | - | Data Lake (Bronze layer) |
| AWS Redshift | - | Data Warehouse |
| Apache Spark | 3.5 | Stream processing |
| dbt | 1.7 | Transformation |
| Python | 3.11 | Core language |
| Docker | 24+ | Containerization |

## Project Structure
```
epl-pipeline/
├── kafka/                  # Kafka + Kafka Connect setup
│   ├── docker-compose.yml
│   └── connectors/         # S3 Sink connector configs
├── airflow/                # Airflow orchestration
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── dags/
│       ├── epl_fetch_standings.py
│       ├── epl_daily_pipeline.py
│       └── epl_matchday_monitor.py
├── src/
│   ├── models/             # Data models (Match, MatchEvent, Standing)
│   ├── schemas/            # JSON Schema validation
│   ├── producers/          # Kafka producers
│   └── utils/              # API client, mapper, kafka utils
└── tests/                  # Unit tests
```

## Data Flow
```
1. Smart Producer fetches EPL data from Football API
2. Data published to Kafka topics:
   - epl.matches    (3 partitions)
   - epl.events     (3 partitions)  
   - epl.standings  (1 partition)
   - epl.injuries   (1 partition)
3. Kafka Connect S3 Sink writes to S3 Data Lake
4. Airflow DAGs orchestrate the pipeline
```

## Kafka Topics

| Topic | Partitions | Retention | Description |
|---|---|---|---|
| epl.matches | 3 | 7 days | Match scores and status |
| epl.events | 3 | 7 days | Goals, cards, substitutions |
| epl.standings | 1 | 7 days | League table |
| epl.injuries | 1 | 7 days | Player injuries |

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| epl_fetch_standings | `0 * * * *` | Fetch standings hourly |
| epl_daily_pipeline | `0 6 * * *` | Daily fixtures + standings |
| epl_matchday_monitor | `*/15 * * * *` | Monitor live scores |

## AWS S3 Structure
```
epl-pipeline-raw/
└── raw/
    ├── epl.matches/
    │   └── year=YYYY/month=MM/day=dd/hour=HH/
    ├── epl.events/
    │   └── year=YYYY/month=MM/day=dd/hour=HH/
    └── epl.standings/
        └── year=YYYY/month=MM/day=dd/
```

## Key Design Decisions

### Why Kafka?
- Decouples producers from consumers — Football API rate limits don't affect downstream processing
- Message retention allows replay if consumers fail
- Partition key = `match_id` ensures all events for the same match stay ordered in the same partition

### Why match_id as Kafka key?
All events for the same match (goals, cards, substitutions) must be processed in order. Using `match_id` as the partition key guarantees they land in the same partition, preserving chronological order.

### Kafka Connect vs custom consumer
Kafka Connect S3 Sink handles offset management, retries, and S3 file rotation automatically. Writing a custom consumer for S3 ingestion would require ~200 lines of boilerplate that Connect handles with a 20-line JSON config.

### context['ds'] vs datetime.now() in Airflow
All DAG tasks use `context['ds']` (execution date) instead of `datetime.now()` to ensure idempotency — re-running a DAG for a past date fetches the correct data for that date.

### Standings idempotency limitation
The Football API does not support fetching historical standings by date (`date` parameter is not available). This means standings are always a real-time snapshot regardless of DAG execution date.

**Workaround:** Two timestamp fields are stored per record:
- `timestamp` — actual API call time (real-time)
- `snapshot_date` — DAG execution date (`context['ds']`)

This allows downstream queries to distinguish between when the data was fetched vs. which pipeline run produced it. For accurate historical standings, a paid API plan with historical data access would be required.

### S3 partitioning strategy
Data is partitioned by `year/month/day/hour` using Kafka Connect's `TimeBasedPartitioner`. This enables:
- Efficient Athena queries with partition pruning
- Glue Crawler to auto-detect schema per partition
- Cost reduction by scanning only relevant partitions

### Error handling
Failed messages go to a Dead Letter Queue (DLQ) topic `epl.dlq` instead of being silently dropped. This allows:
- Visibility into data quality issues
- Manual replay of failed messages
- Alerting on DLQ growth

## Local Setup

### Prerequisites
- Docker Desktop >= 24.x
- Python 3.11+
- AWS account with S3 access

### Quick Start
```bash
# 1. Clone repo
git clone https://github.com/YOUR_USERNAME/epl-pipeline.git
cd epl-pipeline

# 2. Setup environment
cp .env.example .env
# Fill in your API_FOOTBALL_KEY and AWS credentials

# 3. Start Kafka stack
cd kafka && docker compose up -d

# 4. Deploy S3 connectors
cd connectors && ./deploy_connectors.ps1

# 5. Start Airflow
cd ../airflow && docker compose up -d

# 6. Run producer
cd .. && python src/producers/smart_producer.py
```

### Environment Variables
```env
# Football API
API_FOOTBALL_KEY=your_key_here
EPL_SEASON=2024

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=ap-southeast-1
S3_BUCKET_RAW=your-bucket-name
```

## Known Limitations

- **Football API free plan**: Only supports seasons 2022-2024. Season 2025/26 requires paid plan.
- **Standings backfill**: API does not support historical standings by date. All standings are real-time snapshots.
- **Live matches**: Season 2024/25 has ended. Producer runs in mock data mode for development/demo purposes.
- **Single broker**: Local setup uses 1 Kafka broker (replication factor = 1). Production should use 3+ brokers.

## Interview Notes

**Q: Why use Kafka instead of direct API → S3?**
Kafka provides buffering, replay capability, and decoupling. If S3 is slow or down, producers continue without data loss. Multiple consumers (Spark, monitoring, dashboards) can read the same data independently.

**Q: How do you handle late data?**
Kafka's retention (7 days) allows consumers to catch up. For Spark Streaming, watermarking handles late events within a configurable window.

**Q: What would you change for production?**
- 3+ Kafka brokers with replication factor 3
- Schema Registry with Avro instead of JSON Schema
- Paid Football API for real-time 2025/26 data
- Kubernetes instead of Docker Compose
- Proper secrets management (AWS Secrets Manager)

**Q: Why TimeBasedPartitioner for S3?**
Enables Athena partition pruning — queries like `WHERE year=2026 AND month=03` scan only relevant S3 prefixes instead of the entire bucket, reducing cost and query time significantly.