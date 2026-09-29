-- Ball-by-ball match state features for the second (chasing) innings only.
-- This is the training table for the win probability model (phase 3).
--
-- Every feature describes the situation the chasing team faces GOING INTO a
-- delivery (i.e. computed from all strictly-earlier deliveries in that
-- innings), never including the outcome of the delivery itself. This avoids
-- leaking the current ball's result into its own feature row.
--
-- Known simplification: IPL is always a 20-over format, so balls_remaining
-- and wickets_in_hand are hardcoded against 120 legal balls and 10 wickets.
-- Also, matches settled by a Super Over (rare ties) are labeled using the
-- overall match winner even though the normal-time second innings alone
-- ended level -- a known limitation, not worth extra complexity here.

with second_innings as (
    select *
    from {{ ref('stg_deliveries') }}
    where innings_number = 2
),

first_innings_totals as (
    select match_id, sum(runs_total) as first_innings_runs
    from {{ ref('stg_deliveries') }}
    where innings_number = 1
    group by match_id
),

targets as (
    select match_id, first_innings_runs + 1 as target
    from first_innings_totals
),

with_state as (
    select
        si.match_id,
        si.over_number,
        si.ball_number,
        si.batting_team as chasing_team,
        t.target,
        coalesce(
            sum(si.runs_total) over (
                partition by si.match_id
                order by si.over_number, si.ball_number
                rows between unbounded preceding and 1 preceding
            ), 0
        ) as runs_scored_before,
        coalesce(
            sum(case when si.is_legal_delivery then 1 else 0 end) over (
                partition by si.match_id
                order by si.over_number, si.ball_number
                rows between unbounded preceding and 1 preceding
            ), 0
        ) as legal_balls_before,
        coalesce(
            sum(case when si.is_wicket then 1 else 0 end) over (
                partition by si.match_id
                order by si.over_number, si.ball_number
                rows between unbounded preceding and 1 preceding
            ), 0
        ) as wickets_before
    from second_innings si
    join targets t on t.match_id = si.match_id
),

features as (
    select
        match_id,
        chasing_team,
        target,
        greatest(target - runs_scored_before, 0) as runs_needed,
        greatest(120 - legal_balls_before, 0) as balls_remaining,
        greatest(10 - wickets_before, 0) as wickets_in_hand,
        round(runs_scored_before / nullif(legal_balls_before / 6.0, 0), 2) as current_run_rate,
        round(
            greatest(target - runs_scored_before, 0)
            / nullif(greatest(120 - legal_balls_before, 0) / 6.0, 0),
            2
        ) as required_run_rate
    from with_state
)

select
    f.match_id,
    f.chasing_team,
    f.target,
    f.runs_needed,
    f.balls_remaining,
    f.wickets_in_hand,
    f.current_run_rate,
    f.required_run_rate,
    case when m.winner = f.chasing_team then 1 else 0 end as chasing_team_won
from features f
join {{ ref('stg_matches') }} m on m.match_id = f.match_id
where m.winner is not null
