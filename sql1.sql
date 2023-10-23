insert into dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
select 
	du.id as user_id,
	dr.id as restaurant_id,
	dt.id as  timestamp_id,
	object_id as order_key,
	object_value::JSON->>'final_status' as order_status
from stg.ordersystem_orders oo
inner join dds.dm_users du on du.user_id = (oo.object_value->>'user')::JSON->>'id'
inner join dds.dm_restaurants dr on dr.restaurant_id = (oo.object_value->>'restaurant')::JSON->>'id'
inner join dds.dm_timestamps dt on dt.ts = (oo.object_value->>'date')::timestamptz(0)