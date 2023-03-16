-- tables: supplier,lineitem,orders,nation
select
s_name, count(*) as numwait
from orders join
(
  select * from
  lineitem l2 right semi join
  (
    select * from
    lineitem l3 right anti join
    (
      select * from
      lineitem l1 join
      (
        select * from
        supplier join nation
        where s_nationkey = n_nationkey
          and n_name = 'SAUDI ARABIA'
      ) t1
      where t1.s_suppkey = l1.l_suppkey and l1.l_receiptdate > l1.l_commitdate
    ) t2
    on l3.l_orderkey = t2.l_orderkey and l3.l_suppkey <> t2.l_suppkey and l3.l_receiptdate > l3.l_commitdate
  ) t3
  on l2.l_orderkey = t3.l_orderkey and l2.l_suppkey <> t3.l_suppkey
) t4
on o_orderkey = t4.l_orderkey and o_orderstatus = 'F'
group by
    t4.s_name
order by
    numwait desc,
    t4.s_name
limit 100;

