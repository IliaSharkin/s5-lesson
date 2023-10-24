--select 
--	dp.id as product_id,
--	do2.id as order_id,
--    product_item::JSON->>'quantity' as "count",
--	dp.product_price as price,
--	dp.product_price * (product_item::JSON->>'quantity')::int as total_sum
--from (select object_id, json_array_elements((object_value::JSON->>'order_items')::JSON) as product_item from stg.ordersystem_orders oo) as pit
--join dds.dm_products dp on dp.product_id = pit.product_item::JSON->>'id'
--join dds.dm_orders do2 on do2.order_key = pit.object_id


select 
	event_value::JSON->>'order_id' as order_id, 
	json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'product_id' as product_id,
	json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'bonus_payment' as bonus_payment,
	json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'bonus_grant' as bonus_grant
from stg.bonussystem_events be
where event_type = 'bonus_transaction'
