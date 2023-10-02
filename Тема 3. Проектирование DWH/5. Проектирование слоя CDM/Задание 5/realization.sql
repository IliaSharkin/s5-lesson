ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT restaurant_id_settlement_date_unique UNIQUE (restaurant_id, settlement_date);
