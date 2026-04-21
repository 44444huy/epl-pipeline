# Review & Interview Prep Plan

Kế hoạch 5 ngày để review toàn bộ pipeline + chuẩn bị phỏng vấn.

Pattern mỗi ngày: **Read code → Run hands-on → Explain out loud → Self-quiz**.

---

## Day R1 — Kafka (Phase 1)

### Read
- `src/producers/smart_producer.py`, `mock_producer.py`
- `src/models/epl_models.py` — dataclass structure
- `src/schemas/*.json` — JSON Schema validation
- `kafka/docker-compose.yml` — broker + Connect setup
- `kafka/connectors/*.json` — S3 Sink config

### Run (hands-on)
```powershell
cd D:\EPL_PROJECT\epl-pipeline\kafka
docker compose up -d
# Kafka UI: http://localhost:8080
# Send test message:
python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers='localhost:9092'); p.send('epl.matches', b'{\"test\":1}'); p.flush()"
# Xem message trong Kafka UI
```

### Self-quiz (không nhìn README)
1. Tại sao `match_id` làm partition key, không phải `match_date`?
2. Consumer group rebalance xảy ra khi nào? Nếu có 3 consumers và 3 partitions, tắt 1 consumer thì sao?
3. `acks=all` vs `acks=1` vs `acks=0` khác gì? Tradeoff?
4. Kafka Connect lợi gì so với viết custom consumer?
5. DLQ để làm gì? Khi message fail, flow thế nào?

---

## Day R2 — Airflow (Phase 2)

### Read
- `airflow/dags/epl_s3_pipeline.py` — DAG chính 9 tasks
- `airflow/Dockerfile` — dbt venv isolation
- `airflow/docker-compose.yml` — volumes, env vars
- Custom hooks/operators trong `src/` nếu còn

### Run
```powershell
cd D:\EPL_PROJECT\epl-pipeline\airflow
docker compose up -d
# Airflow UI: http://localhost:8081 (admin/admin)
# Trigger DAG → Graph view → click từng task xem logs
```

### Self-quiz
1. `context['ds']` vs `datetime.now()` — tại sao bắt buộc ds? Cho ví dụ backfill fail nếu dùng now().
2. ShortCircuitOperator vs BranchPythonOperator khác gì?
3. TriggerRule default là gì? Khi nào đổi sang `ALL_DONE` hoặc `ONE_FAILED`?
4. XCom lưu ở đâu, size limit bao nhiêu? Khi nào không nên dùng XCom?
5. Retry với `retry_exponential_backoff` hoạt động thế nào?

---

## Day R3 — Spark + AWS (Phase 3)

### Read
- `src/spark/epl_transformer.py` — 3 jobs Kafka → Parquet
- `src/utils/s3_uploader.py`
- `src/utils/glue_catalog.py` — **đặc biệt `setup_all()`** (nơi bug cross-contamination)
- `src/utils/athena_queries.py` — DQ checks

### Run
```powershell
# Trigger epl_s3_pipeline DAG, chờ hoàn tất
# AWS Console → S3 → xem bucket processed/
# Athena:
#   SELECT COUNT(*) FROM epl_db.matches WHERE season='2024/25'
```

### Self-quiz
1. Tại sao `s3a://` cho Spark, `s3://` cho Glue/Athena?
2. Parquet vs JSON vs CSV — kể 3 lý do chọn Parquet cho Bronze.
3. Partition pruning là gì? Athena quét bao nhiêu data nếu query `WHERE season='2024/25' AND matchday=5`?
4. **Bug cross-contamination**: giải thích cho người không biết code, 1 phút. Tại sao phát hiện bằng `SELECT "$path"`?
5. Tại sao không dùng Glue Crawler mà dùng boto3 API?

---

## Day R4 — dbt + Metabase (Phase 4+5)

### Read
- `dbt/epl_dbt/dbt_project.yml`
- `dbt/epl_dbt/models/staging/stg_matches.sql` + `stg_standings.sql`
- `dbt/epl_dbt/models/marts/*.sql`
- `dbt/epl_dbt/models/**/schema.yml` — tests
- `airflow/dbt_profiles/profiles.yml`
- `metabase/docker-compose.yml`

### Run
```powershell
cd D:\EPL_PROJECT\epl-pipeline\dbt\epl_dbt
dbt docs generate --profiles-dir ../../airflow/dbt_profiles
dbt docs serve --profiles-dir ../../airflow/dbt_profiles --port 8082
# Metabase dashboard → View SQL của 1 chart → trace back model
```

### Self-quiz
1. Materialization `view` vs `table` vs `incremental` — khi nào dùng gì?
2. `ref()` vs `source()` khác gì? Tại sao không viết `FROM epl_db.matches` trực tiếp?
3. Silver dedup dùng ROW_NUMBER — viết lại SQL đó không nhìn code.
4. dbt test types: unique, not_null, accepted_values, relationships — cho ví dụ mỗi cái.
5. Tại sao dbt profiles.yml mount vào Airflow container chứ không bake vào Docker image?
6. Metabase connect Athena qua gì? Driver nằm ở đâu?

---

## Day R5 — System design + Interview prep

### Exercise: vẽ architecture từ đầu trên giấy

Không nhìn README, vẽ bằng bút:
- Data flow từ API → dashboard
- Ghi tên mỗi service + vai trò
- Đánh dấu: async vs sync boundaries, storage layers, orchestration

So sánh với `docs/architecture.md` — chỗ nào quên = chỗ cần review thêm.

### Top 15 Interview Questions

Chuẩn bị câu trả lời **< 2 phút** mỗi câu:

1. Tell me about this project in 1 minute.
2. Walk me through the data flow for 1 match event.
3. Why Kafka, not direct API → S3?
4. Why dbt, not raw SQL?
5. How do you guarantee data quality?
6. Tell me about a bug you fixed. _(Glue cross-contamination story)_
7. How do you handle late-arriving data?
8. What happens if the pipeline fails at 3am?
9. How would you scale this to 10x data volume?
10. Why Athena not Redshift/Snowflake?
11. Medallion architecture — why 3 layers?
12. How do you monitor this in production?
13. "What's your biggest learning?" — chuẩn bị 1 câu chuyện cụ thể.
14. "What would you change for production?" — list 5 items sẵn.
15. "Why should we hire you?" — link project to real work value.

---

## Interview Q&A — Sample Answers

### Q1: Tell me about this project in 1 minute
> "I built an end-to-end data pipeline for English Premier League data over 8 weeks. Football API gets ingested into Kafka for buffering, Spark transforms to Parquet on S3, Glue catalogs the partitions, Athena queries them, dbt builds Silver deduplicated views plus Gold business marts, and Metabase visualizes everything on dashboards. Airflow orchestrates 9 tasks end-to-end. Everything's containerized with Docker. The stack cost under a dollar a month on AWS free tier."

### Q6: Tell me about a bug you fixed
> "When I set up Glue Catalog via boto3, I had one `add_partitions()` call taking the S3 base path and registering partitions for both `matches` and `standings` tables. Because both tables live under the same `processed/epl/` prefix, Glue discovered `standings/snapshot_date=...` folders and registered them as **matchday** partitions on the matches table. I ended up with 20 phantom NULL rows in `SELECT * FROM matches`.
>
> I detected it by running `SELECT "$path" FROM matches WHERE matchday IS NULL` — the path column pointed to the standings folder, so I knew the partitions were wrong.
>
> Fix: pass subpath per table — `add_partitions('matches', f'{base}/matches/')` separately from standings. Cleaned up the orphaned partitions with a one-shot script.
>
> The lesson: even 'free' metadata operations need integration tests. A simple `SELECT COUNT(*)` expecting 380 would have caught this immediately."

### Q14: What would you change for production?
1. Kubernetes (EKS) + Helm instead of docker-compose
2. Terraform for S3/Glue/IAM (code review + drift detection)
3. Schema Registry + Avro instead of JSON Schema
4. CDC from Postgres (if upstream allowed) instead of API polling
5. dbt incremental models + snapshots for SCD Type 2 on standings
6. GitHub Actions CI: dbt build + Python lint on every PR
7. Alerting (PagerDuty/Slack) on DAG failure or DQ check fail
8. AWS Secrets Manager thay `.env` file

---

## Flashcards (ngắn hơn — đọc lướt trước phỏng vấn)

| Q | A ngắn |
|---|---|
| Kafka partition key cho matches? | `match_id` — đảm bảo FIFO events cùng trận |
| Lý do chọn Parquet? | Columnar, compressed, Athena scan ít hơn |
| Medallion 3 layer? | Bronze raw / Silver clean + dedup / Gold business |
| Why Athena not Redshift? | Serverless, free tier block Redshift, cheaper for small data |
| DBT test types used? | unique, not_null, accepted_values (~30 tests) |
| Partition pruning? | Filter by partition col → Athena skip irrelevant files |
| Idempotency key trong Airflow? | `context['ds']` không phải `datetime.now()` |
| Glue Catalog vs Crawler? | Boto3 API free + deterministic; Crawler $0.44/DPU/hr |
| Metabase connect Athena? | Community JDBC driver .jar mount vào plugins/ |
| dbt venv trong Airflow? | Conflict protobuf/boto3 → isolated /opt/dbt-venv + symlink |

---

## Bonus resources

- [Designing Data-Intensive Applications](https://dataintensive.net/) — Martin Kleppmann, kinh điển cho DE interview
- [Kafka Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/) — free PDF từ Confluent
- [dbt Learn](https://courses.getdbt.com/) — free official courses
- [System Design Primer](https://github.com/donnemartin/system-design-primer) — practice system design rounds
