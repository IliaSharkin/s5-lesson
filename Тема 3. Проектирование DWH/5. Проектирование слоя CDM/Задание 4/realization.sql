ALTER TABLE cdm.dm_settlement_report
  ALTER COLUMN orders_count SET DEFAULT 0,
  ALTER COLUMN orders_total_sum SET DEFAULT 0,
  ALTER COLUMN orders_bonus_payment_sum SET DEFAULT 0,
  ALTER COLUMN orders_bonus_granted_sum SET DEFAULT 0,
  ALTER COLUMN order_processing_fee SET DEFAULT 0,
  ALTER COLUMN restaurant_reward_sum SET DEFAULT 0,
  ADD CONSTRAINT orders_count_check CHECK (orders_count >= 0),
  ADD CONSTRAINT orders_total_sum_check CHECK (orders_total_sum >= 0),
  ADD CONSTRAINT orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= 0),
  ADD CONSTRAINT orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= 0),
  ADD CONSTRAINT order_processing_fee_check CHECK (order_processing_fee >= 0),
  ADD CONSTRAINT restaurant_reward_sum_check CHECK (restaurant_reward_sum >= 0);
