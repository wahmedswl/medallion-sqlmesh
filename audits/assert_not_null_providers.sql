AUDIT (
  name assert_not_null_providers,
);

SELECT *
FROM @this_model
WHERE
  full_name IS NULL
