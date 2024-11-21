SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
ORDER BY w_warehouse_name ASC, i_item_id ASC
LIMIT 100
