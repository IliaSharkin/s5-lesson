-- Добавляем автоинкрементный первичный ключ
ALTER TABLE cdm.dm_settlement_report
  ADD COLUMN id serial PRIMARY KEY;
