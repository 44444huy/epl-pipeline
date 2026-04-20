# Day 27 — BI Dashboard (Metabase + Athena)

**Goal:** Bổ sung BI layer cho pipeline EPL — dashboards trên Gold tables của dbt.

## Architecture

```
Kafka → Spark → S3 → Glue Catalog → Athena → Metabase → Dashboards
                       ↑
                   dbt (Silver + Gold)

[Metabase metadata: tách riêng → metabase-postgres]
```

## Tech decisions

| Decision | Lý do |
|---|---|
| **Metabase** thay vì Superset | Setup 1 container, Athena driver cộng đồng stable, UI thân thiện hơn cho self-service BI |
| **v0.50.20** pinned | Driver cộng đồng chỉ stable trên v0.50.x; v0.51+ breaking changes plugin API |
| **Metadata DB riêng** (Postgres) | Isolation với Airflow Postgres; teardown độc lập |
| **Plugin mount** thay vì bake image | Đổi driver version không cần rebuild |
| **Shared `epl-network`** | Future-proof: có thể query Airflow Postgres, Kafka metrics |

## Setup

```powershell
# 1. Download Athena driver
cd D:\EPL_PROJECT\epl-pipeline\metabase\plugins
curl -L -o athena.metabase-driver.jar `
  https://github.com/dacort/metabase-athena-driver/releases/download/v1.4.2/athena.metabase-driver.jar

# 2. Start Metabase
cd D:\EPL_PROJECT\epl-pipeline\metabase
docker-compose up -d
```

Mở http://localhost:3000 → Admin setup → Add database (Athena) với:
- Region: `ap-southeast-1`
- S3 staging: `s3://epl-pipeline-processed-nqh/athena-results/`
- Catalog: `AwsDataCatalog`
- Workgroup: `primary`
- Creds: từ `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

## Dashboards

### EPL Season Overview (4 charts)

Full PDF export: [epl_season_overview_dashboard.pdf](screenshots/epl_season_overview_dashboard.pdf)

#### 1. League Table 2024/25
Bảng xếp hạng đầy đủ 20 đội với rank, played, W/D/L, GF/GA, GD, points. Filter theo `season` (dashboard parameter). Source: `epl_gold_epl_gold.mart_team_standings`.

#### 2. Points by Team with League Zones
Bar chart điểm số, **color-coded theo luật EPL**:
- 🟢 Rank 1-4: Champions League (Liverpool 84, Arsenal 74, Man City 71, Chelsea 69)
- 🔵 Rank 5: Europa League (Newcastle 66)
- 🔷 Rank 6: Conference League (Aston Villa 66)
- ⚪ Rank 7-17: Midtable
- 🔴 Rank 18-20: Relegation (Leicester 25, Ipswich 22, Southampton 12)

Pattern: thêm cột `zone` bằng `CASE WHEN rank ...` → Metabase tự color theo series breakout.

#### 3. Match Results Distribution
Pie/donut chart 380 matches: Home Win **41%** / Away Win **35%** / Draw **24%**.

**Insight mùa 2024/25:** Away win cao bất thường so với trung bình lịch sử EPL (~28-30%) → home advantage suy giảm trong mùa này.

#### 4. Matchday Activity & Goals
Combo chart: bar = `matches_played` (trục phải, luôn 10/round), line = `avg_goals_per_match` (trục trái, dao động 1.7-3.9), 38 matchdays.

Matchday 17 thấp nhất (1.7 goals/match) — round phòng ngự chặt nhất mùa.

### Dashboard-level filter

`{{season}}` variable wired vào cả 4 charts → dropdown **Season** đầu dashboard, đổi value → tất cả charts refresh. Future-proof cho khi ingest mùa 2025/26.

## Cost

Mỗi lần load dashboard:
- 4 queries × scan ~0.5MB parquet (Gold tables nhỏ) → ~2MB total
- Athena: $5/TB → **~$0.00001 per dashboard load**
- Metabase caching: Admin → Caching → TTL 1h → giảm Athena calls

## Interview talking points

1. **Tại sao Metabase thay vì Superset/Tableau?** — OSS, low-overhead, 1 container vs. Superset multi-service; Tableau trả tiền + không self-host.
2. **Serverless BI stack** — Metabase → JDBC → Athena → Glue → S3. Không cần provision warehouse, chỉ trả theo scan.
3. **Domain modeling trong viz** — league zones theo luật EPL, không phải hardcoded colors. Thể hiện hiểu business logic.
4. **Caching strategy** — Metabase built-in cache TTL giảm cost; cho dashboard view nhiều lần/ngày.
5. **Separation of concerns** — metadata DB riêng (Postgres) vs Athena (data source); dashboard config version được nếu cần thông qua Metabase API.

## Files

```
metabase/
├── docker-compose.yml    # Metabase + Postgres metadata
├── README.md             # Setup guide
├── .gitignore            # Exclude .jar plugins
└── plugins/
    └── athena.metabase-driver.jar   (gitignored)
```
