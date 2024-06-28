// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_catalog_hive_parquet", "p0,external,hive,external_docker,external_docker_hive") {

    String enable_file_cache = "false"

    def q01 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q01 """
            select 
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
        """
    }

    def q02 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=2"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        sql """set enable_projection=true"""
        qt_q02 """
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
partsupp,
(
  select ps_partkey, min(ps_supplycost) as ps_s from
  partsupp, supplier, nation, region
  where s_suppkey = ps_suppkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
  group by ps_partkey
) t1,
supplier,
part,
nation,
region
where p_partkey = t1.ps_partkey
    and p_partkey = partsupp.ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = t1.ps_s
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
        """
    }

    def q03 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        sql """set enable_projection=true"""
        qt_q03 """
select 
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    (
        select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority, o_custkey from
        lineitem join[shuffle] orders
        where l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
    ) t1 join[shuffle] customer c
    on c.c_custkey = t1.o_custkey
    where c_mktsegment = 'BUILDING'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
        """
    }

    def q04 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=1"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q04 """
select
    o_orderpriority,
    count(*) as order_count
from
    (
        select
            *
        from
            lineitem
        where l_commitdate < l_receiptdate
    ) t1
    right semi join orders
    on t1.l_orderkey = o_orderkey
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
group by
    o_orderpriority
order by
    o_orderpriority;
        """
    }

    def q05 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q05 """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem
    join[shuffle] orders on l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year
    join[shuffle] customer on c_custkey = o_custkey
    join supplier on l_suppkey = s_suppkey and c_nationkey = s_nationkey
    join nation on s_nationkey = n_nationkey
    join region on n_regionkey = r_regionkey and r_name = 'ASIA'
group by n_name
order by revenue desc;
        """
    }

    def q06 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=1"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q06 """
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between .06 - 0.01 and .06 + 0.01
    and l_quantity < 24;
        """
    }

    def q07 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=4"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q07 """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
                lineitem join[shuffle] orders on o_orderkey = l_orderkey and l_shipdate between date '1995-01-01' and date '1996-12-31'
                join[shuffle] customer on c_custkey = o_custkey
                join[shuffle] supplier on s_suppkey = l_suppkey
                join nation n1 on s_nationkey = n1.n_nationkey
                join nation n2 on c_nationkey = n2.n_nationkey
                and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
        """
    }

    def q08 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=1"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q08 """
select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            lineitem,
            orders,
            customer,
            supplier,
            part,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;
        """
    }

    def q09 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=4"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q09 """
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
                lineitem join[shuffle] orders on o_orderkey = l_orderkey
                join[shuffle] partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey
                join[shuffle] part on p_partkey = l_partkey and p_name like '%green%'
                join supplier on s_suppkey = l_suppkey
                join nation on s_nationkey = n_nationkey
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
        """
    }

    def q10 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q10 """
select
    c_custkey,
    c_name,
    sum(t1.l_extendedprice * (1 - t1.l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer join[shuffle]
    (
        select o_custkey,l_extendedprice,l_discount from lineitem join[shuffle] orders
        where l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
    ) t1,
    nation
where
    c_custkey = t1.o_custkey
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
        """
    }

    def q11 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=2"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q11 """
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
        )
order by
    value desc;
        """
    }

    def q12 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=2"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q12 """
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    orders join[shuffle] lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;
        """
    }

    def q13 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=4"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q13 """
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            orders right outer join customer on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
        """
    }

    def q14 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q14 """
select
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    part,
    lineitem
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;
        """
    }

    def q15 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q15 """
with revenue0 as
(select
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
from
    lineitem
where
    l_shipdate >= date '1996-01-01'
    and l_shipdate < date '1996-01-01' + interval '3' month
group by
    l_suppkey)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey;
        """
    }

    def q16 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q16 """
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;
        """
    }

    def q17 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=1"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q17 """
select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem join [broadcast]
    part p1 on p1.p_partkey = l_partkey
where
    p1.p_brand = 'Brand#23'
    and p1.p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem join [broadcast]
            part p2 on p2.p_partkey = l_partkey
        where
            l_partkey = p1.p_partkey
            and p2.p_brand = 'Brand#23'
            and p2.p_container = 'MED BOX'
    );
        """
    }

    def q18 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q18 """
select
    c_name,
    c_custkey,
    t3.o_orderkey,
    t3.o_orderdate,
    t3.o_totalprice,
    sum(t3.l_quantity)
from
customer join
(
  select * from
  lineitem join
  (
    select * from
    orders left semi join
    (
      select
          l_orderkey
      from
          lineitem
      group by
          l_orderkey having sum(l_quantity) > 300
    ) t1
    on o_orderkey = t1.l_orderkey
  ) t2
  on t2.o_orderkey = l_orderkey
) t3
on c_custkey = t3.o_custkey
group by
    c_name,
    c_custkey,
    t3.o_orderkey,
    t3.o_orderdate,
    t3.o_totalprice
order by
    t3.o_totalprice desc,
    t3.o_orderdate
limit 100;
        """
    }

    def q19 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=false"""
        qt_q19 """
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );
        """
    }

    def q20 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q20 """
select
s_name, s_address from
supplier left semi join
(
    select * from
    (
        select l_partkey,l_suppkey, 0.5 * sum(l_quantity) as l_q
        from lineitem
        where l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
        group by l_partkey,l_suppkey
    ) t2 join
    (
        select ps_partkey, ps_suppkey, ps_availqty
        from partsupp left semi join part
        on ps_partkey = p_partkey and p_name like 'forest%'
    ) t1
    on t2.l_partkey = t1.ps_partkey and t2.l_suppkey = t1.ps_suppkey
    and t1.ps_availqty > t2.l_q
) t3
on s_suppkey = t3.ps_suppkey
join nation
where s_nationkey = n_nationkey
    and n_name = 'CANADA'
order by s_name;
        """
    }

    def q21 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=true"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q21 """
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
        """
    }

    def q22 = { 
        sql """set exec_mem_limit=8589934592"""
        sql """set enable_file_cache=${enable_file_cache}"""
        sql """set parallel_fragment_exec_instance_num=8"""
        sql """set disable_join_reorder=false"""
        sql """set enable_cost_based_join_reorder=true"""
        qt_q22 """
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone, 1, 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone, 1, 2) in
                ('13', '31', '23', '29', '30', '18', '17')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone, 1, 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;
        """
    }

    def run_tpch = {
        q01()
        q02()
        q03()
        q04()
        q05()
        q06()
        q07()
        q08()
        q09()
        q10()
        q11()
        q12()
        q13()
        q14()
        q15()
        q16()
        q17()
        q18()
        q19()
        q20()
        q21()
        q22()
    }

    // String enabled = context.config.otherConfigs.get("enableHiveTest")
    // cost too much time in p0, disable it temporary
    String enabled = "false";
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_catalog_${hivePrefix}_parquet"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """switch ${catalog_name}"""
        sql """use `tpch1_parquet`"""

        // without file cache
        enable_file_cache = "false"
        def startTime = System.currentTimeMillis()
        run_tpch()
        def without_cache_time = System.currentTimeMillis() - startTime

        // with file cache, run the first time
        enable_file_cache = "true"
        startTime = System.currentTimeMillis()
        run_tpch()
        def with_cache_first_time = System.currentTimeMillis() - startTime

        // with file cache, run the second time
        enable_file_cache = "true"
        startTime = System.currentTimeMillis()
        run_tpch()
        def with_cache_second_time = System.currentTimeMillis() - startTime

        println("""tpch parquet running time(disable, enable, enable): ${without_cache_time}ms, ${with_cache_first_time}ms, ${with_cache_second_time}ms""")

        sql """drop catalog if exists ${catalog_name}"""
    }
}



