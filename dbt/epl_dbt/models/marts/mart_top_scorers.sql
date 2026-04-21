/*
mart_top_scorers — Gold layer

Top scorers aggregated from event stream.
One row per (player, team, season).

Goal taxonomy (Football-API `detail` field):
  - 'Normal Goal' → counted as regular goal
  - 'Penalty'     → counted (and flagged via penalties column)
  - 'Own Goal'    → EXCLUDED (credits the scorer, not the "scoring" player)
  - 'Missed Penalty' → EXCLUDED (it's logged as a Goal event in API but no actual goal)

Materialized as **table** (Parquet, partitioned by season).
*/

with goals as (
    select *
    from {{ ref('stg_events') }}
    where event_type = 'goal'
      and detail in ('Normal Goal', 'Penalty')
      and player is not null
),

aggregated as (
    select
        player,
        team,
        season,
        count(*) as goals,
        sum(case when detail = 'Penalty' then 1 else 0 end) as penalties,
        sum(case when detail = 'Normal Goal' then 1 else 0 end) as open_play_goals,
        sum(case when half = '1st_half' then 1 else 0 end) as first_half_goals,
        sum(case when half = '2nd_half' then 1 else 0 end) as second_half_goals,
        count(distinct match_id) as matches_scored_in,
        min(minute) as earliest_goal_minute,
        max(minute) as latest_goal_minute
    from goals
    group by player, team, season
)

select
    player,
    team,
    goals,
    penalties,
    open_play_goals,
    first_half_goals,
    second_half_goals,
    matches_scored_in,
    round(goals * 1.0 / matches_scored_in, 2) as goals_per_match_scored,
    earliest_goal_minute,
    latest_goal_minute,
    season  -- partition key last
from aggregated
