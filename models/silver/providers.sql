MODEL (
  name medallion.sl_providers,
  kind FULL,
  cron '@daily',
  grain full_name,
);

SELECT
  'N/A' as community_name,
  'N/A' as practice_name,
  'N/A' as location_name,
  FullName AS full_name,
  Active AS active,
  CreatedAt as created_at
FROM
  medallion.bl_providers
