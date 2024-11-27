import java.text.SimpleDateFormat

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
            CREATE TABLE  ${tableName}(
              time date not null,
              advertiser varchar(10),
              dt date not null,
              channel varchar(10),
              user_id int) 
            DUPLICATE KEY(`time`, `advertiser`)
              PARTITION BY RANGE (dt)(FROM ("2024-07-02") TO ("2024-07-05") INTERVAL 1 DAY)
              -- AUTO PARTITION BY RANGE (date_trunc(`time`, 'day'))()
              distributed BY hash(time) 
              properties("replication_allocation" = "tag.location.default: 1");
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
    def refreshTime = System.currentTimeMillis() / 1000L
    sql """
            CREATE MATERIALIZED VIEW ${mtmvName}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by(dt)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1') 
            AS 
            select dt, advertiser, count(distinct user_id)
                from ${tableName}
                group by dt, advertiser;
    """
    def wait_mtmv_refresh_finish = { refreshMode->
        for (int loop = 0; loop < 300; loop++) {
            Thread.sleep(1200)
            boolean finished = true;
            def result = sql """
select * from tasks('type'='mv') 
where MvName='${mtmvName}' 
AND Status = 'SUCCESS' 
AND CreateTime >= from_unixtime(${refreshTime}) 
AND RefreshMode = '${refreshMode}';"""
            finished = (result.size() == 1)
            if (finished) {
                def sqlResult = result[0]
                log.info("refresh result: ${sqlResult}")
                return;
            }
        }
        throw new Exception("Wait mtmv ${mtmvName} finish timeout.")
    }
    wait_mtmv_refresh_finish("COMPLETE")
    qt_mtmv_init """ SELECT * FROM ${mtmvName} ORDER BY dt, advertiser"""

    sql """INSERT INTO ${tableName} VALUES("2024-07-03",'b', "2024-07-03", 'b',2);"""
    refreshTime = System.currentTimeMillis() / 1000L
    sql """REFRESH MATERIALIZED VIEW ${mtmvName} AUTO;"""
    wait_mtmv_refresh_finish("PARTIAL")
    qt_insert_into_partial_old_partition """ SELECT * FROM ${mtmvName} ORDER BY dt, advertiser"""

    sql """INSERT INTO ${tableName} VALUES("2024-07-04",'b', "2024-07-04", 'a',1);"""
    refreshTime = System.currentTimeMillis() / 1000L
    sql """REFRESH MATERIALIZED VIEW ${mtmvName} AUTO;"""
    wait_mtmv_refresh_finish("PARTIAL")
    qt_insert_into_partial_new_partition """ SELECT * FROM ${mtmvName} ORDER BY dt, advertiser"""

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ DROP MATERIALIZED VIEW ${mtmvName} """

}