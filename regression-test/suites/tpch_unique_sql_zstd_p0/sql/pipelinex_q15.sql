SELECT /*+SET_VAR(experimental_enable_pipeline_x_engine=true) */
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier,
  revenue1
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT max(total_revenue)
    FROM
      revenue1
  )
ORDER BY
  s_suppkey;


