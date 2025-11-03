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

suite("test_runtimefilter_on_datev2", "query_p0") {
    def dateTable = "dateTable"
    def dateV2Table = "dateV2Table"
    def dateTimeTable = "dateTimeTable"
    def dateTimeV2Table = "dateTimeV2Table"
    def dateV2Table2 = "dateV2Table2"
    def dateTimeV2Table2 = "dateTimeV2Table2"

    sql "set enable_runtime_filter_prune=false;"

    sql "DROP TABLE IF EXISTS ${dateTable}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateTable} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateTable} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """

    sql "DROP TABLE IF EXISTS ${dateV2Table}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateV2Table} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateV2Table} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """

    sql "DROP TABLE IF EXISTS ${dateTimeTable}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateTimeTable} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATETIME NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateTimeTable} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """

    sql "DROP TABLE IF EXISTS ${dateTimeV2Table}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateTimeV2Table} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATETIMEV2 NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateTimeV2Table} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """

    sql "DROP TABLE IF EXISTS ${dateV2Table2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateV2Table2} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateV2Table2} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """

    sql "DROP TABLE IF EXISTS ${dateTimeV2Table2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${dateTimeV2Table2} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATETIMEV2 NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dateTimeV2Table2} VALUES (1, "20220101"), (1, "20220101"), (1, "20220101"); """


    sql """ set runtime_filter_type=1; """
    qt_join1 """
        SELECT * FROM ${dateTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join2 """
        SELECT * FROM ${dateTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join3 """
        SELECT * FROM ${dateTimeTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join4 """
        SELECT * FROM ${dateTimeTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join5 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join6 """
        SELECT * FROM ${dateV2Table} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join7 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateTimeV2Table2} b WHERE a.date = b.date;
    """

    qt_join8 """
        SELECT * FROM ${dateV2Table} a, ${dateV2Table2} b WHERE a.date = b.date;
    """

    sql """ set runtime_filter_type=2; """
    qt_join1 """
        SELECT * FROM ${dateTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join2 """
        SELECT * FROM ${dateTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join3 """
        SELECT * FROM ${dateTimeTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join4 """
        SELECT * FROM ${dateTimeTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join5 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join6 """
        SELECT * FROM ${dateV2Table} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join7 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateTimeV2Table2} b WHERE a.date = b.date;
    """

    qt_join8 """
        SELECT * FROM ${dateV2Table} a, ${dateV2Table2} b WHERE a.date = b.date;
    """

    sql """ set runtime_filter_type=4; """
    qt_join1 """
        SELECT * FROM ${dateTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join2 """
        SELECT * FROM ${dateTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join3 """
        SELECT * FROM ${dateTimeTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join4 """
        SELECT * FROM ${dateTimeTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join5 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join6 """
        SELECT * FROM ${dateV2Table} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join7 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateTimeV2Table2} b WHERE a.date = b.date;
    """

    qt_join8 """
        SELECT * FROM ${dateV2Table} a, ${dateV2Table2} b WHERE a.date = b.date;
    """

    sql """ set runtime_filter_type=8; """
    qt_join1 """
        SELECT * FROM ${dateTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join2 """
        SELECT * FROM ${dateTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join3 """
        SELECT * FROM ${dateTimeTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join4 """
        SELECT * FROM ${dateTimeTable} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join5 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateV2Table} b WHERE a.date = b.date;
    """

    qt_join6 """
        SELECT * FROM ${dateV2Table} a, ${dateTimeV2Table} b WHERE a.date = b.date;
    """

    qt_join7 """
        SELECT * FROM ${dateTimeV2Table} a, ${dateTimeV2Table2} b WHERE a.date = b.date;
    """

    qt_join8 """
        SELECT * FROM ${dateV2Table} a, ${dateV2Table2} b WHERE a.date = b.date;
    """

    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_planner=true'
    qt_join1 """
        SELECT * FROM ${dateTable} a, ${dateV2Table} b WHERE a.date = b.date;
    """
}
