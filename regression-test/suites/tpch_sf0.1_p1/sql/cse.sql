select s_suppkey,n_regionkey,(s_suppkey + n_regionkey) + 1 as x, (s_suppkey + n_regionkey) + 2 as y 
from supplier join nation on s_nationkey=n_nationkey order by s_suppkey , n_regionkey limit 10 ;
select s_nationkey,s_suppkey ,(s_nationkey + s_suppkey), (s_nationkey + s_suppkey) + 1,  abs((s_nationkey + s_suppkey) + 1) 
from supplier order by s_suppkey , s_suppkey limit 10 ;
select sum(s_nationkey),sum(s_nationkey +1 ) ,sum(s_nationkey +2 )  , sum(s_nationkey + 3 ) from supplier ;
select sum(s_nationkey),sum(s_nationkey) + count(1) ,sum(s_nationkey) + 2 * count(1) , sum(s_nationkey)  + 3 * count(1) from supplier ;