use tpch1_orc;

CREATE VIEW `view_0` AS
SELECT `v01`.`_c0` AS `max_price`
FROM
  (select max(n_nationkey)
   from `nation`) `v01`;

CREATE VIEW `view_1` AS
select `t1`.`r_name`,
       `t1`.`_c1` as `col1`
from
  (select `region`.`r_name`,
          count(*)
   from `region`
   group by `region`.`r_name`) `t1`
   order by `t1`.`r_name`;

CREATE VIEW `view_2` AS
select `t1`.`r_name`,
       `t1`.`_c1` as `col1`,
       `t1`.`_c2`
from
  (select `region`.`r_name`,
          count(*),
          max(`region`.`r_regionkey`)
   from `region`
   group by `region`.`r_name`) `t1`
   order by `t1`.`r_name`;

CREATE VIEW `view_3` AS select `t2`.`r_name`,
       `t2`.`_c1`,
       `t2`.`_c2`
from
  (select `t1`.`r_name`,
          sum(`t1`.`_c1`),
          sum(`t1`.`_c2`)
   from
     (select `region`.`r_name`,
             count(*),
             max(`region`.`r_regionkey`)
      from `region`
      group by `region`.`r_name`) `t1`
   group by `t1`.`r_name`) `t2`
   order by `t2`.`r_name`;

CREATE VIEW `view_4` AS
select `t1`.`r_name`,
       `t1`.`_c1` as `col1`,
       `t1`.`_c2`
from
  (select `region`.`r_name`,
          count(*),
          max(`region`.`r_regionkey`)
   from `region`
   group by `region`.`r_name`) `t1`;

create view view_5 as
select r_name,
       n.`_c1`
from region r
join
  (select n_regionkey,
          count(*)
   from nation
   group by n_regionkey) n on r.r_regionkey=n.n_regionkey;

create view view_6 as
select r_name,
       n.`_c1`
from region r
join
  (select n_regionkey,
          count(*)
   from nation
   group by n_regionkey
   having count(*) > 2) n on r.r_regionkey=n.n_regionkey;

create view view_7 as
select r_name,
       n.`_c1`
from region r
join
  (select n_regionkey,
          count(*)
   from nation
   group by n_regionkey
   having count(*) > 2) n on r.r_regionkey=n.n_regionkey
order by r_name;