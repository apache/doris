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

suite("distinct_split") {
    sql "set runtime_filter_mode = OFF"
    sql "set disable_join_reorder=true"
    sql "set global enable_auto_analyze=false;"
    sql "drop table if exists test_distinct_multi"
    sql "create table test_distinct_multi(a int, b int, c int, d varchar(10), e date) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test_distinct_multi values(1,2,3,'abc','2024-01-02'),(1,2,4,'abc','2024-01-03'),(2,2,4,'abcd','2024-01-02'),(1,2,3,'abcd','2024-01-04'),(1,2,4,'eee','2024-02-02'),(2,2,4,'abc','2024-01-02');"
    sql "analyze table test_distinct_multi with sync;"
    // first bit 0 means distinct 1 col, 1 means distinct more than 1 col; second bit 0 means without group by, 1 means with group by;
    // third bit 0 means there is 1 count(distinct) in projects, 1 means more than 1 count(distinct) in projects.

    //000 distinct has 1 column, no group by, projection column has 1 count (distinct). four stages agg
    qt_000_count """select count(distinct a) from test_distinct_multi"""

    //001 distinct has 1 column, no group by, and multiple counts (distinct) in the projection column. The two-stage agg is slow for single point calculation in the second stage
    qt_001_count """select count(distinct b), count(distinct a) from test_distinct_multi"""

    //010 distinct has 1 column with group by, and the projection column has 1 count (distinct). two-stage agg. The second stage follows group by hash
    qt_010_count """select count(distinct a) from test_distinct_multi group by b order by 1"""
    qt_010_count_same_column_with_groupby """select count(distinct a) from test_distinct_multi group by a order by 1"""

    //011 distinct has one column with group by, and the projection column has multiple counts (distinct). two stages agg. The second stage follows group by hash
    qt_011_count_same_column_with_groupby """select count(distinct a),count(distinct b)  from test_distinct_multi group by a  order by 1,2"""
    qt_011_count_diff_column_with_groupby """select count(distinct a),count(distinct b)  from test_distinct_multi group by c order by 1,2"""
    qt_011_count_diff_column_with_groupby_multi """select count(distinct a),count(distinct b)  from test_distinct_multi group by a,c order by 1,2"""
    qt_011_count_diff_column_with_groupby_all """select count(distinct a),count(distinct b)  from test_distinct_multi group by a,b,c order by 1,2"""

    //100 distinct columns with no group by, projection column with 1 count (distinct). Three stage agg, second stage gather
    qt_100 """select count(distinct a,b) from test_distinct_multi"""

    //101 distinct has multiple columns, no group by, and multiple counts (distinct) in the projection column (intercept). If the intercept is removed, it can be executed, but the result is incorrect
    qt_101 """select count(distinct a,b), count(distinct a,c) from test_distinct_multi"""
    qt_101_count_one_col_and_two_col """select count(distinct a,b), count(distinct c) from test_distinct_multi"""
    qt_101_count_one_col_and_two_col """select count(distinct a,b), count(distinct a) from test_distinct_multi"""

    //110 distinct has multiple columns, including group by, and the projection column has one count (distinct). three-stage agg. The second stage follows group by hash
    qt_110_count_diff_column_with_groupby """select count(distinct a,b) from test_distinct_multi group by c  order by 1"""
    qt_110_count_same_column_with_groupby1 """select count(distinct a,b) from test_distinct_multi group by a  order by 1"""
    qt_110_count_same_column_with_groupby2 """select count(distinct a,b) from test_distinct_multi group by a,b  order by 1"""

    //111 distinct has multiple columns, including group by, and the projection column has multiple counts (distinct) (intercept). If the intercept is removed, it can be executed, but the result is incorrect
    qt_111_count_same_column_with_groupby1 """select count(distinct a,b), count(distinct a,c) from test_distinct_multi group by c  order by 1,2"""
    qt_111_count_same_column_with_groupby2 """select count(distinct a,b), count(distinct c) from test_distinct_multi group by a,c order by 1,2"""
    qt_111_count_diff_column_with_groupby """select count(distinct a,b), count(distinct a) from test_distinct_multi group by c order by 1,2"""

    // testing other functions
    qt_000_count_other_func """select count(distinct a), max(b),sum(c),min(a) from test_distinct_multi"""
    qt_001_count_other_func """select count(distinct b), count(distinct a), max(b),sum(c),min(a)  from test_distinct_multi"""
    qt_010_count_other_func """select count(distinct a), max(b),sum(c),min(a),b from test_distinct_multi group by b order by 1,2,3,4,5"""
    qt_011_count_other_func """select count(distinct a), count(distinct b),max(b),sum(c),min(a),a  from test_distinct_multi group by a  order by 1,2,3,4,5,6"""
    qt_100_count_other_func """select count(distinct a,b), count(distinct b),max(b),sum(c),min(a) from test_distinct_multi"""
    qt_101_count_other_func """select count(distinct a,b), count(distinct a,c),max(b),sum(c),min(a) from test_distinct_multi"""
    qt_110_count_other_func """select count(distinct a,b),max(b),sum(c),min(a),c from test_distinct_multi group by c  order by 1,2,3,4,5"""
    qt_111_count_other_func """select count(distinct a,b), count(distinct a,c),max(b),sum(c),min(a),c  from test_distinct_multi group by c  order by 1,2,3,4,5,6"""

    // multi distinct three four five
    qt_001_three """select count(distinct b), count(distinct a), count(distinct c) from test_distinct_multi"""
    qt_001_four  """select count(distinct b), count(distinct a), count(distinct c), count(distinct d) from test_distinct_multi"""
    qt_001_five  """select count(distinct b), count(distinct a), count(distinct c), count(distinct d), count(distinct e) from test_distinct_multi"""

    qt_011_three """select count(distinct b), count(distinct a), count(distinct c) from test_distinct_multi group by d order by 1,2,3"""
    qt_011_four """select count(distinct b), count(distinct a), count(distinct c), count(distinct d) from test_distinct_multi group by d order by 1,2,3,4"""
    qt_011_five """select count(distinct b), count(distinct a), count(distinct c), count(distinct d), count(distinct e) from test_distinct_multi group by d order by 1,2,3,4,5"""
    qt_011_three_gby_multi """select count(distinct b), count(distinct a), count(distinct c) from test_distinct_multi group by d,a order by 1,2,3"""
    qt_011_four_gby_multi """select count(distinct b), count(distinct a), count(distinct c), count(distinct d) from test_distinct_multi group by d,c,a order by 1,2,3,4"""
    qt_011_five_gby_multi """select count(distinct b), count(distinct a), count(distinct c), count(distinct d), count(distinct e) from test_distinct_multi group by d,b,a order by 1,2,3,4,5"""

    qt_101_three """select count(distinct a,b), count(distinct a,c) , count(distinct a) from test_distinct_multi"""
    qt_101_four """select count(distinct a,b), count(distinct a,c) , count(distinct a), count(distinct c) from test_distinct_multi"""
    qt_101_five """select count(distinct a,b), count(distinct a,c) , count(distinct a,d), count(distinct c) , count(distinct a,b,c,d) from test_distinct_multi"""

    qt_111_three """select count(distinct a,b), count(distinct a,c) , count(distinct a) from test_distinct_multi group by c  order by 1,2,3"""
    qt_111_four """select count(distinct a,b), count(distinct a,c) , count(distinct a), count(distinct c) from test_distinct_multi group by e order by 1,2,3,4"""
    qt_111_five """select count(distinct a,b), count(distinct a,c) , count(distinct a,d), count(distinct c) , count(distinct a,b,c,d) from test_distinct_multi group by e order by 1,2,3,4,5"""
    qt_111_three_gby_multi """select count(distinct a,b), count(distinct a,c) , count(distinct a) from test_distinct_multi group by c,a  order by 1,2,3"""
    qt_111_four_gby_multi """select count(distinct a,b), count(distinct a,c) , count(distinct a), count(distinct c) from test_distinct_multi group by e,a,b order by 1,2,3,4"""
    qt_111_five_gby_multi """select count(distinct a,b), count(distinct a,c) , count(distinct a,d), count(distinct c) , count(distinct a,b,c,d) from test_distinct_multi group by e,a,b,c,d order by 1,2,3,4,5"""

    // sum has two dimensions: 1. Is there one or more projection columns (0 for one, 1 for more) 2. Is there a group by (0 for none, 1 for yes)
    qt_00_sum """select sum(distinct b) from test_distinct_multi"""
    qt_10_sum """select sum(distinct b), sum(distinct a) from test_distinct_multi"""
    qt_01_sum """select sum(distinct b) from test_distinct_multi group by a order by 1"""
    qt_11_sum """select sum(distinct b), sum(distinct a) from test_distinct_multi group by a order by 1,2"""

    // avg has two dimensions: 1. Is there one or more projection columns (0 for one, 1 for more) 2. Is there a group by (0 for no, 1 for yes)
    qt_00_avg """select avg(distinct b) from test_distinct_multi"""
    qt_10_avg """select avg(distinct b), avg(distinct a) from test_distinct_multi"""
    qt_01_avg """select avg(distinct b) from test_distinct_multi group by a order by 1"""
    qt_11_avg """select avg(distinct b), avg(distinct a) from test_distinct_multi group by a order by 1,2"""

    //group_concat
    sql """select group_concat(distinct d order by d)  from test_distinct_multi"""
    sql """select group_concat(distinct d order by d), group_concat(distinct cast(a as string) order by cast(a as string))  from test_distinct_multi"""
    sql """select group_concat(distinct d order by d)  from test_distinct_multi group by a order by 1"""
    sql """select group_concat(distinct d order by d), group_concat(distinct cast(a as string) order by cast(a as string))  from test_distinct_multi  group by a order by 1,2"""

    // mixed distinct function
    qt_count_sum_avg_no_gby "select sum(distinct b), count(distinct a), avg(distinct c) from test_distinct_multi"
    qt_count_multi_sum_avg_no_gby "select sum(distinct b), count(distinct a,d), avg(distinct c) from test_distinct_multi"
    qt_count_sum_avg_with_gby "select sum(distinct b), count(distinct a), avg(distinct c) from test_distinct_multi group by b,a order by 1,2,3"
    qt_count_multi_sum_avg_with_gby "select sum(distinct b), count(distinct a,d), avg(distinct c) from test_distinct_multi  group by a,b order by 1,2,3"

    // There is a reference query in the upper layer
    qt_multi_sum_has_upper """select c1+ c2 from (select sum(distinct b) c1, sum(distinct a) c2 from test_distinct_multi) t"""
    qt_000_count_has_upper """select abs(c1) from (select count(distinct a) c1 from test_distinct_multi) t"""
    qt_010_count_has_upper """select c1+100 from (select count(distinct a) c1 from test_distinct_multi group by b) t order by 1"""
    qt_011_count_diff_column_with_groupby_all_has_upper """select max(c2), max(c1) from (select count(distinct a) c1,count(distinct b) c2 from test_distinct_multi group by a,b,c) t"""
    qt_100_has_upper """select c1+1 from (select count(distinct a,b) c1 from test_distinct_multi) t where c1>0"""
    qt_101_has_upper """select c1+c2+100 from (select count(distinct a,b) c1, count(distinct a,c) c2 from test_distinct_multi) t"""
    qt_111_count_same_column_with_groupby1_has_upper """select max(c1), min(c2) from (select count(distinct a,b) c1, count(distinct a,c) c2 from test_distinct_multi group by c) t"""
    qt_010_count_sum_other_func_has_upper """select sum(c0),max(c1+c2), min(c2+c3+c4),max(b)  from (select sum(distinct b) c0,count(distinct a) c1, max(b) c2,sum(c) c3,min(a) c4,b from test_distinct_multi group by b) t"""
    qt_010_count_other_func_has_upper"""select sum(c0), max(c1+c2), min(c2+c3+c4),max(b)  from (select count(distinct b) c0,count(distinct a) c1, max(b) c2,sum(c) c3,min(a) c4,b from test_distinct_multi group by b) t"""

    // In cte or in nested cte.
    qt_cte_producer """with t1 as (select a,b from test_distinct_multi)
    select count(distinct t.a), count(distinct tt.b) from t1 t cross join t1 tt;"""
    qt_cte_consumer """with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1 t cross join t1 tt;"""
    qt_cte_multi_producer """
    with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3;
    """
    qt_multi_cte_nest """
    with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3, (with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3) tmp;
    """
    qt_multi_cte_nest2 """
    with t1 as (with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3, (with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3) tmp)
    select * from t1,t1,(with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3, (with t1 as (select count(distinct a), count(distinct b) from test_distinct_multi),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3) tmp) t
    """
    qt_cte_consumer_count_multi_column_with_group_by """with t1 as (select count(distinct a,b), count(distinct b,c) from test_distinct_multi group by d)
    select * from t1 t cross join t1 tt order by 1,2,3,4;"""
    qt_cte_consumer_count_multi_column_without_group_by """with t1 as (select sum(distinct a), count(distinct b,c) from test_distinct_multi)
    select * from t1 t cross join t1 tt;"""
    qt_cte_multi_producer_multi_column """
    with t1 as (select count(distinct a), count(distinct b,d) from test_distinct_multi group by c),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi group by c),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3 order by 1,2,3,4,5,6;
    """
    qt_cte_multi_nested """
    with tmp as (with t1 as (select count(distinct a), count(distinct b,d) from test_distinct_multi group by c),
    t2 as (select sum(distinct a), sum(distinct b) from test_distinct_multi group by c),
    t3 as (select sum(distinct a), count(distinct b) from test_distinct_multi)
    select * from t1,t2,t3)
    select * from tmp, (select sum(distinct a), count(distinct b,c) from test_distinct_multi) t, (select sum(distinct a), count(distinct b,c) from test_distinct_multi group by d) tt order by 1,2,3,4,5,6,7,8,9,10
    """

    // multi aggregate
    qt_2_agg_count_distinct """select count(distinct c1) c3, count(distinct c2) c4 from (select count(distinct a,b) c1, count(distinct a,c) c2 from test_distinct_multi group by c) t"""
    qt_3_agg_count_distinct """select count(distinct c3), count(distinct c4) from (select count(distinct c1) c3, count(distinct c2) c4 from (select count(distinct a,b) c1, count(distinct a,c) c2 from test_distinct_multi group by c) t) tt"""

    // shape
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    qt_multi_count_without_gby """explain shape plan select count(distinct b), count(distinct a) from test_distinct_multi"""
    qt_multi_sum_without_gby """explain shape plan select sum(distinct b), sum(distinct a) from test_distinct_multi"""
    qt_sum_count_without_gby """explain shape plan select sum(distinct b), count(distinct a) from test_distinct_multi"""
    qt_multi_count_mulitcols_without_gby """explain shape plan select count(distinct b,c), count(distinct a,b) from test_distinct_multi"""
    qt_multi_count_mulitcols_with_gby """explain shape plan select count(distinct b,c), count(distinct a,b) from test_distinct_multi group by d"""
    qt_three_count_mulitcols_without_gby """explain shape plan select count(distinct b,c), count(distinct a,b), count(distinct a,b,c) from test_distinct_multi"""
    qt_four_count_mulitcols_with_gby """explain shape plan select count(distinct b,c), count(distinct a,b),count(distinct b,c,d), count(distinct a,b,c) from test_distinct_multi group by d"""
    qt_has_other_func "explain shape plan select count(distinct b), count(distinct a), max(b),sum(c),min(a)  from test_distinct_multi"
    qt_2_agg """explain shape plan select max(c1), min(c2) from (select count(distinct a,b) c1, count(distinct a,c) c2 from test_distinct_multi group by c) t"""

    qt_multi_count_with_gby """explain shape plan select count(distinct b), count(distinct a) from test_distinct_multi group by c"""
    qt_multi_sum_with_gby """explain shape plan select sum(distinct b), sum(distinct a) from test_distinct_multi group by c"""
    qt_sum_count_with_gby """explain shape plan select sum(distinct b), count(distinct a) from test_distinct_multi group by a"""
    qt_has_grouping """explain shape plan select count(distinct b), count(distinct a) from test_distinct_multi group by grouping sets((a,b),(c));"""

    //----------------test null hash join ------------------------
    sql "drop table if exists test_distinct_multi_null_hash;"
    sql "create table test_distinct_multi_null_hash(a int, b int, c int, d varchar(10), e date) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test_distinct_multi_null_hash values(1,null,null,null,'2024-12-08');"
    qt_null_hash "SELECT a, b, count(distinct c,e), count(distinct concat(d,e))/count(distinct e) FROM test_distinct_multi_null_hash where e = '2024-12-08' GROUP BY a, b;"

    qt_same_distinct_arg "select sum(distinct b), count(distinct b) from test_distinct_multi;"
    qt_same_distinct_arg_2group "select count(distinct b), sum(distinct a), count(distinct a) from test_distinct_multi;"
    qt_same_distinct_arg_shape """explain shape plan select sum(distinct b), count(distinct b) from test_distinct_multi;"""
    qt_same_distinct_arg_2group_shape """explain shape plan select count(distinct b), sum(distinct a), count(distinct a) from test_distinct_multi;"""

    multi_sql """drop table if exists sales_records;
    CREATE TABLE sales_records (
            region VARCHAR(20),
                    city VARCHAR(20),
            product_category VARCHAR(20),
                    product_id INT,
            sales_amount DECIMAL(10,2),
                    sales_date DATE,
            customer_id INT
    ) DISTRIBUTED BY HASH(region) PROPERTIES('replication_num'='1');

    -- 插入测试数据
    INSERT INTO sales_records VALUES
    ('North', 'Beijing', 'Electronics', 101, 1500.00, '2024-01-15', 1001),
    ('North', 'Beijing', 'Electronics', 101, 1200.00, '2024-01-16', 1002),
    ('North', 'Beijing', 'Clothing', 201, 800.00, '2024-01-15', 1003),
    ('North', 'Tianjin', 'Electronics', 102, 2000.00, '2024-01-17', 1004),
    ('North', 'Tianjin', 'Clothing', 202, 600.00, '2024-01-18', 1005),
    ('South', 'Shanghai', 'Electronics', 103, 1800.00, '2024-01-16', 1006),
    ('South', 'Shanghai', 'Electronics', 103, 1600.00, '2024-01-17', 1007),
    ('South', 'Shanghai', 'Clothing', 203, 900.00, '2024-01-18', 1008),
    ('South', 'Guangzhou', 'Electronics', 104, 2200.00, '2024-01-19', 1009),
    ('South', 'Guangzhou', 'Clothing', 204, 750.00, '2024-01-20', 1010),
    ('East', 'Nanjing', 'Electronics', 105, 1300.00, '2024-01-21', 1011),
    ('East', 'Nanjing', 'Clothing', 205, 850.00, '2024-01-22', 1012);"""
    sql "set multi_distinct_strategy=1;"
    qt_use_multi_distinct """SELECT
    region,
    city,
    product_category,
    COUNT(DISTINCT customer_id) AS unique_customers,
            COUNT(DISTINCT product_id) AS unique_products,
            SUM(sales_amount) AS total_sales,
            AVG(sales_amount) AS avg_sale_amount,
            COUNT(*) AS total_transactions
    FROM sales_records
    GROUP BY GROUPING SETS (
            (region, city),
    (region, product_category),
    (product_category),
    ()
    )
    order by 1,2,3,4,5,6,7,8
    ;"""

    sql "set multi_distinct_strategy=2;"
    qt_use_cte_split """SELECT
    region,
    city,
    product_category,
    COUNT(DISTINCT customer_id) AS unique_customers,
            COUNT(DISTINCT product_id) AS unique_products,
            SUM(sales_amount) AS total_sales,
            AVG(sales_amount) AS avg_sale_amount,
            COUNT(*) AS total_transactions
    FROM sales_records
    GROUP BY GROUPING SETS (
            (region, city),
    (region, product_category),
    (product_category),
    ()
    )
    order by 1,2,3,4,5,6,7,8
    ;"""
}
