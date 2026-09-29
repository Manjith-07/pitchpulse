-- One row per delivery, cleaned from raw_deliveries with added phase/boundary flags.
select
    match_id,
    innings_number,
    initcap(trim(batting_team)) as batting_team,
    over_number,
    ball_number,
    batter,
    bowler,
    non_striker,
    runs_batter,
    runs_extras,
    runs_total,
    extras_type,
    -- wides and no-balls don't count against the 6-ball over
    (
        extras_type is null
        or (extras_type not like '%wides%' and extras_type not like '%noballs%')
    ) as is_legal_delivery,
    is_wicket,
    wicket_kind,
    player_dismissed,
    (runs_batter in (4, 6)) as is_boundary,
    case
        when over_number + 1 <= 6 then 'powerplay'
        when over_number + 1 <= 15 then 'middle'
        else 'death'
    end as phase
from {{ source('raw', 'raw_deliveries') }}
