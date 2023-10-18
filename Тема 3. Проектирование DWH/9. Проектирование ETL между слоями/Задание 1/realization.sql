with dm_of as (
select
	id,
	order_key,
	order_status,
	restaurant_id,
	timestamp_id,
	user_id
from dds.dm_orders
where order_status = 'CLOSED'
)
insert into cdm.dm_settlement_report (
	restaurant_id,
	restaurant_name,
	settlement_date,
	orders_count,
	orders_total_sum,
	orders_bonus_payment_sum, 
	orders_bonus_granted_sum,
	order_processing_fee,
	restaurant_reward_sum
)
	select 
		dm_of.restaurant_id,
		restaurant_name, 
	    sd."date" as settlement_date,
		count(distinct fps.order_id) as orders_count,
		sum(fps.total_sum) as orders_total_sum,
		sum(bonus_payment) as orders_bonus_payment_sum,
		sum(bonus_grant) as orders_bonus_granted_sum,
		sum(fps.total_sum) * 0.25 as order_processing_fee,
		sum(fps.total_sum) - (sum(fps.total_sum) * 0.25) - sum(bonus_payment) as restaurant_reward_sum
	from dds.fct_product_sales fps 
	join dm_of on dm_of.id = fps.order_id
	join dds.dm_timestamps sd on sd.id = dm_of.timestamp_id
	join dds.dm_restaurants dr on dr.id = dm_of.restaurant_id
	group by dm_of.restaurant_id, restaurant_name, settlement_date
on conflict (restaurant_id, settlement_date) do update
set
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;

