/*
mart_team_standings — Gold layer

Latest league standings per team per season.
One row per team (most recent snapshot_date).

Adds:
  - win_pct: win rate as percentage (easier to read than 0.xxx)
  - form_rating: simple rating = points per game played

Materialized as TABLE (Parquet on S3, registered in Glue epl_gold database).
Partitioned by season → Athena partition pruning.

Interview note:
  This is the "Gold" layer — business-ready, denormalized, optimized for analytics.
  Downstream: Metabase/Grafana connects here directly.
*/

with latest_snapshot as (
    -- Keep only the most recent snapshot per team per season
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
        row_number() over (
            partition by team, season
            order by snapshot_date desc
        ) as rn
    from {{ ref('stg_standings') }}
),

enriched as (
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
        round(win_rate * 100, 1)                              as win_pct,
        case
            when played > 0 then round(cast(points as double) / played, 2)
            else 0.0
        end                                                   as points_per_game,
        snapshot_date                                         as last_updated,
        season                                                -- partition key: PHẢI là cột cuối cùng (Athena/Hive rule)
    from latest_snapshot
    where rn = 1
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
    win_pct,
    points_per_game,
    last_updated,
    season            -- partition key last
from enriched
order by rank asc
