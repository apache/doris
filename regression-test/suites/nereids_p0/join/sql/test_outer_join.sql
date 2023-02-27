select b.k1, c.k1 from test_join b right join test_join c on b.k1 = c.k1 and 2=4  order by 1,2;
select b.k1, c.k1 from test_join b left join test_join c on b.k1 = c.k1 and 2=4  order by 1,2;
