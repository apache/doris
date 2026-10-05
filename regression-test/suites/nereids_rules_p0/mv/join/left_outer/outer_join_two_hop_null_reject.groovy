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

suite("outer_join_two_hop_null_reject") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_materialized_view_rewrite=true"
    sql "set enable_nereids_timeout=false"

    sql """drop table if exists fact_orders_2hop"""
    sql """drop table if exists dim_stores_2hop"""
    sql """drop table if exists dim_regions_2hop"""

    sql """
        CREATE TABLE IF NOT EXISTS fact_orders_2hop (
          order_date DATE NOT NULL,
          store_id INT NOT NULL,
          amount DECIMALV3(10, 2) NOT NULL
        )
        DUPLICATE KEY(order_date, store_id)
        DISTRIBUTED BY HASH(store_id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS dim_stores_2hop (
          id INT NOT NULL,
          store_name VARCHAR(32) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS dim_regions_2hop (
          store_id INT NOT NULL,
          region_name VARCHAR(32) NOT NULL
        )
        DUPLICATE KEY(store_id)
        DISTRIBUTED BY HASH(store_id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql """
        insert into fact_orders_2hop values
        ('2024-01-01', 1, 100.00),
        ('2024-01-01', 2, 200.00),
        ('2024-01-02', 3, 300.00),
        ('2024-01-02', 4, 400.00)
    """

    sql """
        insert into dim_stores_2hop values
        (1, 'Store A'),
        (2, 'Store B'),
        (3, 'Store C')
    """

    sql """
        insert into dim_regions_2hop values
        (1, 'West'),
        (2, 'East'),
        (3, 'West')
    """

    sql """analyze table fact_orders_2hop with sync"""
    sql """analyze table dim_stores_2hop with sync"""
    sql """analyze table dim_regions_2hop with sync"""

    def compare_res = { def stmt ->
        sql "set enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "set enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def mvName = "mv_orders_2hop_null_reject"
    def mvSql = """
        select o.order_date, o.store_id, d.store_name, r.region_name,
               sum(o.amount) as total_amount
        from fact_orders_2hop o
        left join dim_stores_2hop d
          on o.store_id = d.id
        left join dim_regions_2hop r
          on d.id = r.store_id
        group by o.order_date, o.store_id, d.store_name, r.region_name
    """
    def querySql = """
        select o.order_date, sum(o.amount)
        from fact_orders_2hop o
        left join dim_stores_2hop d
          on o.store_id = d.id
        left join dim_regions_2hop r
          on d.id = r.store_id
        where r.region_name = 'West'
        group by o.order_date
        order by 1
    """

    create_async_mv(db, mvName, mvSql)
    mv_rewrite_success_without_check_chosen(querySql, mvName)
    compare_res(querySql)

    sql """drop materialized view if exists ${mvName}"""
}
