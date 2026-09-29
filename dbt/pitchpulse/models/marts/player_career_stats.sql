-- Career batting and bowling aggregates per player.
-- Note: balls_faced/balls_bowled use is_legal_delivery (excludes wides and
-- no-balls), which is a simplification of official scoring rules where
-- no-balls do still count as a ball faced by the batter.
with batting_stats as (
    select
        batter as player,
        sum(runs_batter) as runs_scored,
        count(*) filter (where is_legal_delivery) as balls_faced,
        count(*) filter (where is_wicket and player_dismissed = batter) as dismissals,
        sum(case when is_boundary then 1 else 0 end) as boundaries_hit
    from {{ ref('stg_deliveries') }}
    group by batter
),

bowling_stats as (
    select
        bowler as player,
        count(*) filter (where is_legal_delivery) as balls_bowled,
        -- byes and leg byes are not charged against the bowler
        sum(
            case
                when extras_type like '%byes%' or extras_type like '%legbyes%' then 0
                else runs_total
            end
        ) as runs_conceded,
        -- run outs are not credited to the bowler
        count(*) filter (
            where is_wicket
                and (wicket_kind is null or wicket_kind not ilike '%run out%')
        ) as wickets_taken
    from {{ ref('stg_deliveries') }}
    group by bowler
)

select
    coalesce(b.player, bo.player) as player,
    b.runs_scored,
    b.balls_faced,
    round(b.runs_scored * 100.0 / nullif(b.balls_faced, 0), 2) as batting_strike_rate,
    b.dismissals,
    b.boundaries_hit,
    bo.balls_bowled,
    bo.runs_conceded,
    bo.wickets_taken,
    round(bo.runs_conceded / nullif(bo.balls_bowled / 6.0, 0), 2) as economy_rate
from batting_stats b
full outer join bowling_stats bo on b.player = bo.player
