create table public.systems_filtered as
-- sistemas que encostaram na borda ("grossa") da caixa de 150 km (REMOVER)
with no_border_systems as (
	select name
	from systems s 
	where st_overlaps(st_envelope(geom), st_polygonfromtext('POLYGON((-61.300000 -4.450000, -58.700000 -4.450000, -58.700000 -1.810000, -61.300000 -1.810000, -61.300000 -4.450000))', 4326))
),
-- sistemas que tiveram a camada de 40 dBZ em algum momento (MANTER)
two_layers_systems as (
	select distinct name
	from systems
	where nlayers > 0
),
-- sistemas espontaneos que só tem uma linha e não se juntaram em outro sistema (REMOVER)
single_event_systems as (
	select name
	from systems s
	group by name
	having count(*) = 1
),
single_spontaneous_systems as (
	select "name"::varchar as name
	from systems
	where
		event = 'SPONTANEOUS_GENERATION'
		and name in (select name from single_event_systems)
),
exploded_systems as (
	select
		name,
		unnest(relations) as original_system
	from systems
),
merged_single_spontaneous_systems as (
	select
		original_system
	from exploded_systems
	where
		original_system in (select name from single_spontaneous_systems)
),
exclude_systems as (
	select
		name
	from single_spontaneous_systems
	where
		name not in (select original_system from merged_single_spontaneous_systems)
)
select *
from systems
where
	name not in (select distinct name from no_border_systems)
	and name in (select name from two_layers_systems)
	and name::varchar not in (select name from exclude_systems);