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
suite("advance_mv") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tbName1 = "test_advance_mv_agg_table"
    def tbName2 = "test_advance_mv_dup_table"
    def tbName3 = "schema_change_dup_mv_regression_test"

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql "DROP TABLE IF EXISTS ${tbName1} FORCE"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                k1 int, 
                k2 int, 
                k3 int, 
                v1 varchar(10) replace, 
                v2 bigint sum
            )
            AGGREGATE KEY(k1, k2, k3)
            DISTRIBUTED BY HASH(k1) buckets 1 properties("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbName2} FORCE"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2}(
                k1 int, 
                k2 int, 
                k3 int, 
                k4 varchar(10)
            )
            DISTRIBUTED BY HASH(k1) buckets 1 properties("replication_num" = "1");
        """
    sql "DROP TABLE IF EXISTS ${tbName3} FORCE"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3} (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",
                    `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                    `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                    `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
                DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 1
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );
    """

    sql """INSERT INTO ${tbName3} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20);
    """
    sql"""INSERT INTO ${tbName3} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 19);
    """
    sql """INSERT INTO ${tbName3} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21);
    """
    sql """INSERT INTO ${tbName3} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20);
    """

    sql """insert into ${tbName1} values (1,1,1,'a',10);"""
    sql """insert into ${tbName1} values (2,2,2,'b',10);"""
    sql """insert into ${tbName1} values (3,3,3,'c',10);"""

    sql """insert into ${tbName2} values (4,4,4,'d');"""
    sql """insert into ${tbName2} values (5,5,5,'e');"""
    sql """insert into ${tbName2} values (6,6,6,'f');"""

    sql "analyze table ${tbName1} with sync;"
    sql "analyze table ${tbName2} with sync;"
    sql "analyze table ${tbName3} with sync;"
    sql """set enable_stats=false;"""

    create_sync_mv(db, tbName1, "mv1", "SELECT k1 as a1, sum(v2) as a2 FROM ${tbName1} GROUP BY k1;")

    explain {
        sql("select k1, sum(v2) from ${tbName1} group by k1 order by k1;")
        contains "(mv1)"
    }

    sql """set enable_stats=true;"""
    explain {
        sql("select k1, sum(v2) from ${tbName1} group by k1 order by k1;")
        contains "(mv1)"
    }
    order_qt_select_star "select k1 from ${tbName1} order by k1;"

    create_sync_mv(db, tbName2, "mv2", "SELECT abs(k1)+k2+1 as a4, sum(abs(k2+2)+k3+3) as a3 FROM ${tbName2} GROUP BY a4;")

    explain {
        sql("SELECT abs(k1)+k2+1 tmp, sum(abs(k2+2)+k3+3) FROM ${tbName2} GROUP BY tmp;")
        contains "(mv2)"
    }
    sql """set enable_stats=false;"""
    explain {
        sql("SELECT abs(k1)+k2+1 tmp, sum(abs(k2+2)+k3+3) FROM ${tbName2} GROUP BY tmp;")
        contains "(mv2)"
    }
    order_qt_select_star "SELECT abs(k1)+k2+1 tmp, sum(abs(k2+2)+k3+3) FROM ${tbName2} GROUP BY tmp;"

    create_sync_mv(db, tbName2, "mv3", "SELECT abs(k1)+k2+1 as a5, abs(k2+2)+k3+3 as a6 FROM ${tbName2};")

    explain {
        sql("SELECT abs(k1)+k2+1 tmp, abs(k2+2)+k3+3 FROM ${tbName2};")
        contains "(mv3)"
    }
    order_qt_select_star "SELECT abs(k1)+k2+1 tmp, abs(k2+2)+k3+3 FROM ${tbName2};"

    sql """set enable_stats=true;"""
    explain {
        sql("SELECT abs(k1)+k2+1 tmp, abs(k2+2)+k3+3 FROM ${tbName2};")
        contains "(mv3)"
    }

    create_sync_mv(db, tbName3, "mv4", "select date as b1, user_id as b2, city as b3, sum(age) from ${tbName3} group by date, user_id, city;")

    explain {
        sql("select sum(age) from ${tbName3};")
        contains "(mv4)"
    }
    order_qt_select_star "select sum(age) from ${tbName3};"

    sql """set enable_stats=false;"""
    explain {
        sql("select sum(age) from ${tbName3};")
        contains "(mv4)"
    }
}
