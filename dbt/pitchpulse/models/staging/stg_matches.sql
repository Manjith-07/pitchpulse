-- One row per match, cleaned and standardized from raw_matches.
select
    match_id,
    match_type,
    season,
    match_date,
    venue,
    city,
    initcap(trim(team1)) as team1,
    initcap(trim(team2)) as team2,
    initcap(trim(toss_winner)) as toss_winner,
    toss_decision,
    initcap(trim(winner)) as winner,
    win_by_type,
    win_by_margin,
    player_of_match
from {{ source('raw', 'raw_matches') }}
