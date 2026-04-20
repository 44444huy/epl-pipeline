# Metabase + Athena — EPL Pipeline BI Layer

Metabase dashboards trên Gold tables (dbt output trên Athena).

## Architecture
```
S3 (parquet) → Glue Catalog → Athena → Metabase → Dashboards
                                          ↑
                         (Metabase metadata → Postgres riêng)
```

## Setup (1 lần)

### 1. Download Athena community driver
Driver không có trong Metabase OSS nên phải tải riêng vào `plugins/`:

```powershell
# Trong PowerShell
cd D:\EPL_PROJECT\epl-pipeline\metabase\plugins
curl -L -o athena.metabase-driver.jar `
  https://github.com/dacort/metabase-athena-driver/releases/download/v1.4.2/athena.metabase-driver.jar
```

Hoặc tải manual: https://github.com/dacort/metabase-athena-driver/releases (chọn `.jar` mới nhất).

### 2. Start Metabase
```powershell
cd D:\EPL_PROJECT\epl-pipeline\metabase
docker-compose up -d
docker logs -f metabase   # chờ dòng "Metabase Initialization COMPLETE"
```

Lần đầu start ~60-90s (khởi tạo metadata DB). Xong mở http://localhost:3000

### 3. Admin setup (qua UI)
- Tạo admin account (email + password bất kỳ, chỉ local)
- Skip "add data" wizard

### 4. Connect Athena
Settings → Admin settings → Databases → Add database:
- **Database type:** Amazon Athena
- **Display name:** EPL Athena
- **Region:** `ap-southeast-1`
- **Access key / Secret key:** (copy từ `.env`)
- **S3 staging directory:** `s3://epl-pipeline-processed-nqh/athena-results/`
- **Catalog:** `awsdatacatalog`
- **Workgroup:** `primary`

Save → Metabase sẽ sync Glue Catalog (thấy `epl_db` và `epl_gold`).

## Dashboards (Day 27 deliverable)

Xây 4 dashboard trên schema `epl_gold`:

| Dashboard | Source table | Chart type |
|---|---|---|
| League Table | `mart_team_standings` | Table + bar chart (points) |
| Match Results Distribution | `mart_match_results` | Pie (Home/Draw/Away %) |
| Goals Analysis | `mart_team_standings` | Bar (GF/GA per team) |
| Matchday Activity | `mart_match_results` | Line (matches per matchday) |

## Troubleshooting

**Driver không load:**
- Check `docker logs metabase | grep -i athena` — phải thấy "Loading plugin athena"
- Đảm bảo file `.jar` trong `plugins/` (không phải subfolder)

**Connection timeout:**
- Check AWS creds trong `.env` còn valid
- Check IAM user có `AmazonAthenaFullAccess` + `AmazonS3ReadOnlyAccess`

**Metabase chậm:**
- Query Athena có cost → enable caching: Admin → Caching → TTL 1 hour
