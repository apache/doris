SELECT n_name,
       r_name
FROM   nation,
       region
WHERE  r_regionkey > n_nationkey 
