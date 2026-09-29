-- Average run rate by phase (powerplay/middle/death), grouped by team and season.
select
    d.batting_team as team,
    m.season,
    d.phase,
    sum(d.runs_total) as total_runs,
    count(*) filter (where d.is_legal_delivery) as legal_balls,
    round(
        sum(d.runs_total) / nullif(count(*) filter (where d.is_legal_delivery) / 6.0, 0),
        2
    ) as average_run_rate
from {{ ref('stg_deliveries') }} d
join {{ ref('stg_matches') }} m on d.match_id = m.match_id
group by d.batting_team, m.season, d.phase
order by team, season, phase
