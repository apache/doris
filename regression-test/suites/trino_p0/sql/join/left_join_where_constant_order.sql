SELECT n_name, r_name
FROM   nation
       LEFT JOIN region
              ON n_nationkey = r_regionkey
WHERE  r_name > 'G'

