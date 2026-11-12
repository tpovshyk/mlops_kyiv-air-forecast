-- Кількість рядків порції в кожному шарі. Виклик: make counts MONTH=2025-01-01
SELECT 'raw' AS layer, count(*) AS rows
  FROM raw.saveecobot_hourly WHERE batch_start = :'month'::timestamptz
UNION ALL
SELECT 'dwh', count(*)
  FROM dwh.pm25_hourly
 WHERE ts >= :'month'::timestamptz AND ts < :'month'::timestamptz + interval '1 month'
UNION ALL
SELECT 'features', count(*)
  FROM features.pm25_hourly
 WHERE ts >= :'month'::timestamptz AND ts < :'month'::timestamptz + interval '1 month';
