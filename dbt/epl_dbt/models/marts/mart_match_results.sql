/*
mart_match_results — Gold layer

Finished match results enriched with derived metrics.
One row per match (finished matches only).

Adds:
  - winner: winning team name (NULL for draw)
  - loser: losing team name (NULL for draw)
  - is_high_scoring: true if total_goals >= 4
  - goal_diff: absolute goal difference

Materialized as TABLE (Parquet on S3, registered in Glue epl_gold database).
Partitioned by season → Athena partition pruning.
*/

with finished as (
    select *
    from {{ ref('stg_matches') }}
    where status = 'finished'
),

enriched as (
    select
        match_id,
        home_team,
        away_team,
        home_score,
        away_score,
        total_goals,
        result,

        -- Derived: winner / loser
        case
            when result = 'home_win' then home_team
            when result = 'away_win' then away_team
            else null
        end as winner,

        case
            when result = 'home_win' then away_team
            when result = 'away_win' then home_team
            else null
        end as loser,

        -- Derived: goal difference (absolute)
        abs(home_score - away_score) as goal_diff,

        -- Derived: high scoring flag
        case when total_goals >= 4 then true else false end as is_high_scoring,

        venue,
        matchday,
        event_time,
        season        -- partition key: PHẢI là cột cuối cùng (Athena/Hive rule)
    from finished
)

select
    match_id,
    home_team,
    away_team,
    home_score,
    away_score,
    total_goals,
    result,
    winner,
    loser,
    goal_diff,
    is_high_scoring,
    venue,
    matchday,
    event_time,
    season            -- partition key last
from enriched
