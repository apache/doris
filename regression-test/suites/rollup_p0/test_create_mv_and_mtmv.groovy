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

suite("test_create_mv_and_mtmt") {
    def tableName = "test_create_mv_and_mtmt_advertiser_view_record"
    def mvName = "test_create_mv_and_mtmt_advertiser_uv"
    def mtmvName = "test_create_mv_and_mtmt_advertiser_uv_mtmv"
    sql """
            CREATE TABLE ${tableName}(
              time date not null,
              advertiser varchar(10),
              dt date not null,
              channel varchar(10),
              user_id int) 
            DUPLICATE KEY(`time`, `advertiser`)
              PARTITION BY RANGE (dt)(FROM ("2024-07-02") TO ("2024-07-04") INTERVAL 1 DAY)
              -- AUTO PARTITION BY RANGE (date_trunc(`time`, 'day'))()
              distributed BY hash(time) 
              properties("replication_num" = "1");
    """
    sql """ insert into ${tableName} values("2024-07-02",'a', "2024-07-02", 'a',1); """
    sql """ insert into ${tableName} values("2024-07-03",'b', "2024-07-03", 'b',1); """

    createMV("""
        CREATE materialized VIEW ${mvName} AS
        SELECT advertiser,
               channel,
               dt,
               bitmap_union(to_bitmap(user_id))
        FROM ${tableName}
        GROUP BY advertiser,
                 channel,
                 dt;

    """)
    // Hit sync mv when setting enable_sync_mv_cost_based_rewrite as false
    sql "set enable_sync_mv_cost_based_rewrite=false;"
    explain {
        sql("""
                    SELECT dt,advertiser,
                          count(DISTINCT user_id)
                    FROM ${tableName}
                    GROUP BY dt,advertiser""")
        contains "(${mvName})"
    }
    sql """
            CREATE MATERIALIZED VIEW ${mtmvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(dt)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1') 
            AS 
            select dt, advertiser, count(distinct user_id)
                from ${tableName}
                group by dt, advertiser;
    """
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ DROP MATERIALIZED VIEW ${mtmvName} """

}