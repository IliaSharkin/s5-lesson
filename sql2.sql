with kek as (
select distinct
    object_id,
	json_array_elements(object_value::JSON->'menu')->>'_id' as product_id, 
	json_array_elements(object_value::JSON->'menu')->>'name' as product_name,
	(json_array_elements(object_value::JSON->'menu')->>'price')::numeric  as product_price,
	update_ts as active_from,
	'2099-12-31 00:00:00.000'::timestamp as active_to
from stg.ordersystem_restaurants or2)
select product_id, product_name, product_price, kek.active_from, kek.active_to, dmp.id from kek
join dds.dm_restaurants dmp on dmp.restaurant_id = kek.object_id