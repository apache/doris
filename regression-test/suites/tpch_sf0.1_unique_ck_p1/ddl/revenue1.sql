CREATE VIEW IF NOT EXISTS revenue1 AS
  SELECT
    l_suppkey AS supplier_no,
    sum(l_extendedprice * (1 - l_discount)) AS total_revenue
  FROM
    lineitem
  WHERE
    l_shipdate >= DATE '1996-01-01'
    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
GROUP BY
  l_suppkey;
