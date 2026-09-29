-- Win percentage by team, grouped by venue. Ties and no-results (winner is
-- null) are excluded since they don't count as a win or a loss.
with team_matches as (
    select match_id, venue, team1 as team, (winner = team1) as won
    from {{ ref('stg_matches') }}
    where winner is not null

    union all

    select match_id, venue, team2 as team, (winner = team2) as won
    from {{ ref('stg_matches') }}
    where winner is not null
)

select
    team,
    venue,
    count(*) as matches_played,
    sum(case when won then 1 else 0 end) as matches_won,
    round(sum(case when won then 1 else 0 end) * 100.0 / count(*), 2) as win_percentage
from team_matches
group by team, venue
order by team, venue
