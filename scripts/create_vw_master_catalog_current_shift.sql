-- BigQuery: view with only rows whose publish_time falls in the *current* shift (IST).
-- Source: vw_master_catalog (adjust project/dataset if yours differs).
--
-- Shift rules (Asia/Kolkata):
--   Shift I:  08:00:00 inclusive  →  before 20:00:00 (8 PM) same calendar day
--   Shift II: 20:00:00 inclusive →  before 08:00:00 next calendar day
--
-- The view re-evaluates "current shift" on every query (uses CURRENT_TIMESTAMP()).

CREATE OR REPLACE VIEW `alubee-prod.alubee_production_marts.vw_master_catalog_current_shift` AS
WITH
  now_bounds AS (
    SELECT
      DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata') AS now_ist,
      DATE(CURRENT_TIMESTAMP(), 'Asia/Kolkata') AS today_ist
  ),
  shift_window AS (
    SELECT
      CASE
        -- Shift I: today 08:00 IST → today 20:00 IST (exclusive end)
        WHEN EXTRACT(HOUR FROM now_ist) >= 8 AND EXTRACT(HOUR FROM now_ist) < 20 THEN
          TIMESTAMP(DATETIME(today_ist, TIME(8, 0, 0)), 'Asia/Kolkata')
        -- Shift II (evening): today 20:00 IST → tomorrow 08:00 IST
        WHEN EXTRACT(HOUR FROM now_ist) >= 20 THEN
          TIMESTAMP(DATETIME(today_ist, TIME(20, 0, 0)), 'Asia/Kolkata')
        -- Shift II (after midnight, before 08:00): yesterday 20:00 IST → today 08:00 IST
        ELSE
          TIMESTAMP(DATETIME(DATE_SUB(today_ist, INTERVAL 1 DAY), TIME(20, 0, 0)), 'Asia/Kolkata')
      END AS shift_start_ts,
      CASE
        WHEN EXTRACT(HOUR FROM now_ist) >= 8 AND EXTRACT(HOUR FROM now_ist) < 20 THEN
          TIMESTAMP(DATETIME(today_ist, TIME(20, 0, 0)), 'Asia/Kolkata')
        WHEN EXTRACT(HOUR FROM now_ist) >= 20 THEN
          TIMESTAMP(DATETIME(DATE_ADD(today_ist, INTERVAL 1 DAY), TIME(8, 0, 0)), 'Asia/Kolkata')
        ELSE
          TIMESTAMP(DATETIME(today_ist, TIME(8, 0, 0)), 'Asia/Kolkata')
      END AS shift_end_ts
    FROM now_bounds
  )
SELECT
  src.*,
  CASE
    WHEN SAFE_CAST(src.measurement AS INT64) = 16 THEN 'break'
    WHEN SAFE_CAST(src.measurement AS INT64) = 32 THEN 'shot'
    WHEN SAFE_CAST(src.measurement AS INT64) = 45 THEN 'without notice'
    WHEN SAFE_CAST(src.measurement AS INT64) = 18 THEN 'maintenance'
    WHEN SAFE_CAST(src.measurement AS INT64) = 5 THEN 'power cut'
    WHEN SAFE_CAST(src.measurement AS INT64) = 19 THEN 'setting'
    WHEN SAFE_CAST(src.measurement AS INT64) = 34 THEN 'mould'
    WHEN SAFE_CAST(src.measurement AS INT64) = 33 THEN 'reset'
    WHEN SAFE_CAST(src.measurement AS INT64) = 4 THEN 'manpower'
    WHEN SAFE_CAST(src.measurement AS INT64) = 17 THEN 'no load'
    ELSE NULL
  END AS description
  ,mm.Machine_no AS machine_no
  ,mm.Unit AS unit
  ,mm.Machine_Type AS type
FROM `alubee-prod.alubee_production_marts.vw_master_catalog` AS src
CROSS JOIN shift_window AS w
LEFT JOIN `alubee-prod.alubee_production_marts.dim_machine_mapper` AS mm
  ON mm.Device_ID = SAFE_CAST(src.device_id AS INT64)
WHERE src.publish_time >= w.shift_start_ts
  AND src.publish_time < w.shift_end_ts

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY src.device_id, src.measurement
  ORDER BY src.publish_time DESC
) = 1;
