/*
stg_matches — Silver layer

Dedup bronze matches: keep latest record per match_id.

Bronze table may have duplicates because:
  - Smart Producer sends updates as match progresses (score changes)
  - Backfill DAG may re-send same matchday data

Design: ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY event_time DESC)
  → keep the most recent snapshot per match (final score)

Materialized as VIEW → no S3 write, always reads fresh from bronze.
*/

with source as (
    select * from {{ source('epl_bronze', 'matches') }}
    -- Staging layer responsibility: lọc bad data trước khi dedup
    -- Bronze có thể có: null match_id (partial writes), matchday non-integer (wrong partition)
    where match_id is not null
      and home_team is not null
      and away_team is not null
      and try_cast(matchday as integer) is not null   -- loại rows có matchday = date string
),

deduped as (
    select
        match_id,
        home_team,
        away_team,
        home_score,
        away_score,
        status,
        venue,
        total_goals,
        result,
        season,
        cast(matchday as integer) as matchday,
        event_time,
        row_number() over (
            partition by match_id
            order by event_time desc
        ) as rn
    from source
)

select
    match_id,
    home_team,
    away_team,
    home_score,
    away_score,
    status,
    venue,
    total_goals,
    result,
    season,
    matchday,
    event_time
from deduped
where rn = 1
