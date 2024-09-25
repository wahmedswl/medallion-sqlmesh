MODEL (
  name medallion.gl_providers_by_practice,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column created_at
  ),
  start '2020-01-01',
  cron '@daily',
  grain (
    full_name
  )
);

SELECT
  practice_name,
  active,
  NULL AS created_at,
  COUNT(full_name) AS total_providers
FROM medallion.sl_providers
WHERE
  created_at BETWEEN @start_date AND @end_date
GROUP BY
  practice_name,
  active