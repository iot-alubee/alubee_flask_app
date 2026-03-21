-- BigQuery: create monthly_planner table (run in alubee-prod)
-- Dataset: alubee_production_marts

CREATE TABLE IF NOT EXISTS `alubee-prod.alubee_production_marts.dim_monthly_planner` (
  plan_id          INT64 NOT NULL,
  plan_month       STRING NOT NULL,
  department       STRING NOT NULL,
  part_no          STRING NOT NULL,
  part_name        STRING NOT NULL,
  schedule         INT64 NOT NULL,
  opening_qty      INT64 NOT NULL,
  balance_to_be_produced INT64 NOT NULL,
  priority         STRING NOT NULL,
  allocated        INT64 NOT NULL,
  produced         INT64,
  created_at       TIMESTAMP
)
OPTIONS(
  description = 'PPC Monthly Planner plans'
);
