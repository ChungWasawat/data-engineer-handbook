with deduplicate as (
    select *, row_number() over(partition by game_id, team_id, player_id order by updated_at desc, created_at desc ) as row_num
    from game_details
)
select * from deduplicate
where row_num = 1