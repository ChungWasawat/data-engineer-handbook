-- '->' = return values as JSON, '->>' = return values as text: col1 -> 'key` ->> 1(position in list)
-- '#>', '#>>' similar to '->', '->>' but the syntax is different: col1 #> '{key1, key2}'

select 
	v.properties ->> 'player_name',
	e.object_identifier ,
	cast(v.properties #>> '{total_points}' as real) / 
	coalesce (case when cast(v.properties->>'number_of_games' as real) = 0 then 1 else cast(v.properties->>'number_of_games' as real) end, 1) as avg_points,
	e.properties ->> 'subject_points',
	e.properties ->> 'num_games'
from vertices v 
join edges e on e.subject_identifier = v.identifier and e.subject_type = v.type
where e.object_type = 'player'::vertex_type

-- use number as index
select v.properties -> 'teams' ->> 0 from vertices v

-- show type of value in json's key
select json_typeof(v.properties -> 'player_name') from vertices v

-- #> #>> and json_extract_path, 
select v.properties #>> '{teams}', json_typeof( v.properties #> '{teams}' ) from vertices v

select json_extract_path(v.properties, 'teams'), json_typeof( json_extract_path(v.properties, 'teams' ) ) from vertices v

select v.properties #>> '{teams, 0}'  from vertices v

select json_extract_path(v.properties, 'teams', '0'), json_typeof( json_extract_path(v.properties, 'teams', '0' ) ) from vertices v

-- extract keys from json type
select json_object_keys(v.properties) from vertices v

-- create json from rows
select row_to_json(row(a)) from arena a 

-- copy content from file into table
copy table_name from file