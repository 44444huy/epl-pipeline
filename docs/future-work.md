# Future Work / Roadmap

Items deferred from the main build. Each entry includes enough context to pick up cold.

---

## 1. Medallion refactor — Spark reads from S3 raw bronze

**Status:** Designed + partially coded, reverted. See conversation Day 30.

**Current flow (legacy):**
```
Kafka ─┬─→ Kafka Connect → S3 raw  (write-only archive, not consumed downstream)
       └─→ Spark → S3 processed
```
S3 raw is a dead archive. Spark bypasses it and reads Kafka directly, which couples
processing to Kafka retention (7 days) and prevents replay of older data.

**Target flow:**
```
Kafka → Kafka Connect → S3 raw (bronze) → Spark → S3 processed (silver) → dbt → gold
```

**Implementation notes:**
- Kafka Connect S3 Sink writes newline-delimited JSON (jsonl), 1 record per line.
- Path layout: `raw/epl.{topic}/year=*/month=*/day=*/[hour=*/]*.json` (matches/events
  partitioned by hour, standings by day).
- Spark reader pattern:
  ```python
  spark.read.schema(SCHEMA) \
      .option("recursiveFileLookup", "true") \
      .json(f"{raw_path}/epl.{topic}/")
  ```
  `recursiveFileLookup=true` ignores Connect's wallclock partitions (year/month/day/hour
  are sink-time, not domain-time — irrelevant for analytics).
- Add `--source s3_raw --raw-path s3a://epl-pipeline-raw-nqh/raw` CLI args to
  `epl_transformer.py`.
- Update `epl_s3_pipeline` DAG to add `wait_connect_flush` task before Spark.
  Kafka Connect flush settings: `flush.size=10`, `rotate.interval.ms=60000`.
  Sleep 90s = 1.5x rotate interval, safe for any in-progress batch.
- Verify connector states (RUNNING, not FAILED) before triggering Spark.

**Blocker hit during attempt:** Airflow container couldn't reach `kafka-connect:8083` —
Airflow and Kafka stacks on different Docker networks. Fix: `docker network connect`
or merge docker-compose stacks into one network.

**Why deferred:** Network plumbing + 2-3h test cycle. Not on critical path for MVP report.

---

## 2. Events in daily pipeline DAG (auto-fetch)

**Status:** Manual backfill only.

**Inconsistency:** `epl_daily_pipeline` (scheduled 6am) auto-fetches matches +
standings, but events require manual trigger of `epl_backfill_events_dag`.

**Fix:** Add `fetch_todays_events` task to `epl_daily_pipeline.py`:
- Input: today's fixtures (already fetched in `fetch_todays_fixtures`)
- Filter: `status in ("FT", "AET", "PEN")` — only finished matches have full events
- Loop: `client.get_fixture_events(fixture_id)` per match
- Map via `map_event_to_match_event` → push to Kafka `epl.events`
- API cost: ~10 finished fixtures/day × 1 events call = 10 req/day (within 100/day quota)

**Flow update:**
```
fetch_todays_fixtures → fetch_todays_events
                              ↓
                       pipeline_summary
```

---

## 3. Backfill remaining matchdays (10–38)

**Status:** Only matchdays 1–9 have events on S3.

**Reason:** API free tier = 100 req/day. Each matchday costs ~11 requests (10 matches
+ 1 fixtures call). 9 matchdays consumed ~99 requests in one day.

**Plan:** 3 more days of backfill via `epl_backfill_events` DAG with config:
- Day 1: `{"matchday_start": 10, "matchday_end": 18}` (~99 req)
- Day 2: `{"matchday_start": 19, "matchday_end": 27}` (~99 req)
- Day 3: `{"matchday_start": 28, "matchday_end": 38}` (~110 req — may need to split)

`event_id` is deterministic (since Day 29 fix), so safe to re-run any matchday
without duplicates.

**Acceptance:** Athena `SELECT COUNT(*) FROM events GROUP BY matchday` shows all 38
matchdays present with ~10k–15k total events.

---

## 4. Connector health-check task in pipeline DAG

**Status:** Not implemented.

**Problem:** Kafka Connect S3 sink could silently FAIL → no new raw data lands → Spark
processes stale data → analytics drift. Currently only caught by manual inspection.

**Fix (depends on #1):** The `wait_connect_flush` task already drafted for medallion
refactor checks all 3 connectors are RUNNING. Even without #1, add this as a
standalone task in `epl_s3_pipeline`:
```python
def check_connectors_healthy():
    expected = {"epl-matches-s3-sink", "epl-events-s3-sink", "epl-standings-s3-sink"}
    # GET http://kafka-connect:8083/connectors/{name}/status
    # raise if connector or any task != RUNNING
```

Run after `check_kafka` to fail fast before Spark wastes compute.

---

## Priority order

1. **#3 Backfill** — easiest win, just trigger DAG over 3 days. Get full season for
   real interview demos.
2. **#2 Events daily** — small DAG edit, makes events first-class citizen alongside
   matches/standings.
3. **#4 Connector health-check** — small, reduces silent-failure risk.
4. **#1 Medallion refactor** — biggest lift but most architectural value. Needs
   docker-compose network fix first.
