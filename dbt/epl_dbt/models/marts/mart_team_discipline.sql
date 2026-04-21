/*
mart_team_discipline — Gold layer

Card counts per team per season.
One row per (team, season).

Card taxonomy comes from api_mapper normalization:
  event_type = 'yellow_card'  → yellows
  event_type = 'red_card'     → reds (includes 2nd-yellow → red)

Materialized as **table** (Parquet, partitioned by season).
*/

with cards as (
    select *
    from {{ ref('stg_events') }}
    where event_type in ('yellow_card', 'red_card')
),

aggregated as (
    select
        team,
        season,
        count(*) as total_cards,
        sum(case when event_type = 'yellow_card' then 1 else 0 end) as yellow_cards,
        sum(case when event_type = 'red_card' then 1 else 0 end) as red_cards,
        count(distinct match_id) as matches_with_cards
    from cards
    group by team, season
)

select
    team,
    total_cards,
    yellow_cards,
    red_cards,
    matches_with_cards,
    round(total_cards * 1.0 / nullif(matches_with_cards, 0), 2) as cards_per_match,
    season  -- partition key last
from aggregated
