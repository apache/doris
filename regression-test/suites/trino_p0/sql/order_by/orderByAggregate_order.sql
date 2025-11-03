-- database: presto; groups: orderby
select n_regionkey as n_nationkey, n_nationkey as n_regionkey, n_name from nation where n_nationkey < 20 order by n_nationkey desc, n_regionkey asc
