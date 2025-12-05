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

suite("iceberg_branch_complex_queries", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_complex"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """drop database if exists ${catalog_name}.test_db_complex force"""
    sql """create database ${catalog_name}.test_db_complex"""
    sql """ use ${catalog_name}.test_db_complex """

    String table_name = "test_complex_branch"

    sql """ drop table if exists ${table_name} """
    sql """ create table ${table_name} (id int, name string, value int, category string) """

    sql """ insert into ${table_name} values (1, 'a', 10, 'cat1'), (2, 'b', 20, 'cat1'), (3, 'c', 30, 'cat2'), (4, 'd', 40, 'cat2'), (5, 'e', 50, 'cat3') """
    sql """ alter table ${table_name} create branch b1_complex """
    sql """ insert into ${table_name}@branch(b1_complex) values (6, 'f', 60, 'cat3'), (7, 'g', 70, 'cat1'), (8, 'h', 80, 'cat2') """

    // Test 1.4.1: Aggregate queries
    qt_agg_count """ select count(*) from ${table_name}@branch(b1_complex) """
    qt_agg_sum """ select sum(value) from ${table_name}@branch(b1_complex) """
    qt_agg_avg """ select avg(value) from ${table_name}@branch(b1_complex) """
    qt_agg_max """ select max(value) from ${table_name}@branch(b1_complex) """
    qt_agg_min """ select min(value) from ${table_name}@branch(b1_complex) """
    qt_agg_group_by """ select category, count(*), sum(value) from ${table_name}@branch(b1_complex) group by category order by category """

    // Test 1.4.2: Window function queries
    qt_window_row_number """ select id, name, value, row_number() over (order by value) as rn from ${table_name}@branch(b1_complex) order by id """
    qt_window_rank """ select id, name, value, rank() over (partition by category order by value) as rk from ${table_name}@branch(b1_complex) order by id """
    qt_window_dense_rank """ select id, name, value, dense_rank() over (order by value) as dr from ${table_name}@branch(b1_complex) order by id """

    // Test 1.4.3: Multi-table join queries
    String table_name2 = "test_complex_branch2"
    sql """ drop table if exists ${table_name2} """
    sql """ create table ${table_name2} (id int, info string) """
    sql """ insert into ${table_name2} values (1, 'info1'), (2, 'info2'), (3, 'info3'), (6, 'info6'), (7, 'info7') """
    sql """ alter table ${table_name2} create branch b2_complex """
    sql """ insert into ${table_name2}@branch(b2_complex) values (8, 'info8'), (9, 'info9') """

    qt_join_branch_branch """ select t1.id, t1.name, t2.info from ${table_name}@branch(b1_complex) t1 join ${table_name2}@branch(b2_complex) t2 on t1.id = t2.id order by t1.id """
    qt_join_branch_main """ select t1.id, t1.name, t2.info from ${table_name}@branch(b1_complex) t1 join ${table_name2} t2 on t1.id = t2.id order by t1.id """
    qt_join_main_branch """ select t1.id, t1.name, t2.info from ${table_name} t1 join ${table_name2}@branch(b2_complex) t2 on t1.id = t2.id order by t1.id """

    // Test 1.4.4: Subquery with branch
    qt_subquery_in """ select * from ${table_name} where id in (select id from ${table_name}@branch(b1_complex) where value > 50) order by id """
    qt_subquery_exists """ select * from ${table_name} t1 where exists (select 1 from ${table_name}@branch(b1_complex) t2 where t2.id = t1.id and t2.value > 50) order by id """
    qt_subquery_scalar """ select id, name, (select max(value) from ${table_name}@branch(b1_complex)) as max_val from ${table_name} order by id limit 5 """

    // Test 1.4.5: CTE with branch
    qt_cte_simple """ with cte as (select * from ${table_name}@branch(b1_complex) where value > 50) select * from cte order by id """
    qt_cte_multiple """ 
        with cte1 as (select * from ${table_name}@branch(b1_complex) where category = 'cat1'),
             cte2 as (select * from ${table_name}@branch(b1_complex) where category = 'cat2')
        select * from cte1 union all select * from cte2 order by id 
    """
    qt_cte_join """ 
        with branch_data as (select * from ${table_name}@branch(b1_complex)),
             main_data as (select * from ${table_name})
        select b.id, b.name, m.value from branch_data b left join main_data m on b.id = m.id order by b.id 
    """

    // Test complex filter with branch
    qt_complex_filter """ 
        select category, count(*), avg(value) 
        from ${table_name}@branch(b1_complex) 
        where value > 30 and name like '%f%' or category in ('cat1', 'cat2')
        group by category 
        order by category 
    """

    // Test order by and limit with branch
    qt_order_limit """ select * from ${table_name}@branch(b1_complex) order by value desc limit 3 """
    qt_order_by_multiple """ select * from ${table_name}@branch(b1_complex) order by category, value desc """
}

