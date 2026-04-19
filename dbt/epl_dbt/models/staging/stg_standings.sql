/*
stg_standings — Silver layer

Dedup bronze standings: keep latest record per team + snapshot_date.

Bronze table may have duplicates because:
  - Daily pipeline fetches standings each run
  - Backfill DAG may re-send same snapshot_date data

Design: ROW_NUMBER() OVER (PARTITION BY team, snapshot_date ORDER BY event_time DESC)
  → keep freshest snapshot per team per day

Materialized as VIEW → no S3 write, always reads fresh from bronze.
*/

with source as (
    select * from {{ source('epl_bronze', 'standings') }}
    -- Staging layer responsibility: lọc bad data trước khi dedup
    -- Bronze có thể có: null team/rank/points (partial writes từ API lỗi)
    where team is not null
      and rank is not null
      and points is not null
      and season is not null
),

deduped as (
    select
        team,
        rank,
        played,
        won,
        drawn,
        lost,
        goals_for,
        goals_against,
        goal_diff,
        points,
        cast(win_rate as double) as win_rate,
        season,
        snapshot_date,
        event_time,
        row_number() over (
            partition by team, snapshot_date
            order by event_time desc
        ) as rn
    from source
    where snapshot_date is not null
)

select
    team,
    rank,
    played,
    won,
    drawn,
    lost,
    goals_for,
    goals_against,
    goal_diff,
    points,
    win_rate,
    season,
    snapshot_date,
    event_time
from deduped
where rn = 1
