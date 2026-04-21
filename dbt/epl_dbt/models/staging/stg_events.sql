/*
stg_events — Silver layer

Dedup bronze events: keep latest record per event_id.

Bronze table may have duplicates because:
  - Smart Producer sends events as match progresses (live updates)
  - Backfill DAG may re-send same fixture's events

Design: ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC)
  → keep the most recent snapshot per event.

Also normalizes event_type to lowercase for consistent filtering
(Football-API returns mixed case: 'Goal', 'Card', 'subst').

Materialized as VIEW → always fresh from bronze.
*/

with source as (
    select * from {{ source('epl_bronze', 'events') }}
    -- Guard against bad data: null keys, invalid matchday
    where event_id is not null
      and match_id is not null
      and try_cast(matchday as integer) is not null
),

deduped as (
    select
        event_id,
        match_id,
        lower(event_type) as event_type,
        minute,
        team,
        player,
        detail,
        half,
        season,
        cast(matchday as integer) as matchday,
        event_time,
        row_number() over (
            partition by event_id
            order by event_time desc
        ) as rn
    from source
)

select
    event_id,
    match_id,
    event_type,
    minute,
    team,
    player,
    detail,
    half,
    season,
    matchday,
    event_time
from deduped
where rn = 1
