MODEL (
  name medallion.gl_providers_by_community,
  kind FULL,
  cron '@daily',
  grain (community_name,active),
  audits (
    not_null(columns := (community_name))
  )
);

SELECT
  community_name,
  active,
  count(full_name) total_providers
FROM
  medallion.sl_providers
GROUP BY community_name, active
