// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite(common_cte) {
    sql set enable_common_cte=true;
    qt_1  explain verbose select * from (select count() a from lineitem union all select count() b from lineitem) T;
}


explain verbose select * from (select count() a from lineitem  union all select count() b from lineitem where orderkey > 0) T


set enable_common_cte=false;
explain verbose
with x as (select count() a from lineitem)
select * from (select * from x union all select * from x) T;


CREATE MATERIALIZED VIEW vlo 
BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY RANDOM BUCKETS 2
PROPERTIES ('replication_num' = '1') 
AS select lineitem.L_LINENUMBER, t2.O_CUSTKEY, t2.O_ORDERSTATUS 
    from lineitem 
    inner join 
    (select * from orders where O_ORDERSTATUS = 'o') t2 
    on lineitem.L_ORDERKEY = t2.O_ORDERKEY ;


select lineitem.L_LINENUMBER 
from lineitem 
inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY 
where orders.O_ORDERSTATUS = 'o'

-------------
CREATE MATERIALIZED VIEW v_filter 
BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
DISTRIBUTED BY RANDOM BUCKETS 2
PROPERTIES ('replication_num' = '1') 
AS select o_custkey, o_orderkey
    from orders where O_ORDERSTATUS = 'o' or O_ORDERSTATUS = 'x' ;

explain 
select o_custkey, o_orderkey
    from orders where O_ORDERSTATUS = 'o';