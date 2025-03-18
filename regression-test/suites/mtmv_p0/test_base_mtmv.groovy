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

suite("test_base_mtmv","mtmv") {
    def tableName = "t_test_base_mtmv_user"
    def newTableName = "t_test_base_mtmv_user_new"
    def mvName = "multi_mv_test_base_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${newTableName}`"""

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE TABLE IF NOT EXISTS `${newTableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        INSERT INTO ${tableName} VALUES("2022-10-26",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """
        INSERT INTO ${newTableName} VALUES("2022-10-27",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """drop materialized view if exists ${mvName};"""
    String querySql = "SELECT event_day,id,username FROM ${tableName}";

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        ${querySql};
    """
    def jobName = getJobName("regression_test_mtmv_p0", mvName);
    order_qt_init "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"

    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")

    // add column
    sql """
        alter table ${tableName} add COLUMN new_col INT AFTER username;
    """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName}"))
    order_qt_add_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"

    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // rename column
    sql """
        alter table ${tableName} rename COLUMN new_col new_col_1;
    """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName}"))
    order_qt_rename_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // modify column
    sql """
        alter table ${tableName} modify COLUMN new_col_1 BIGINT;
    """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName}"))
    order_qt_modify_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // drop column
    sql """
        alter table ${tableName} drop COLUMN new_col_1;
    """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName}"))
    order_qt_drop_column "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // replace table
     sql """
    ALTER TABLE ${tableName} REPLACE WITH TABLE ${newTableName} PROPERTIES('swap' = 'false');
    """
    order_qt_replace_table "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // rename table
     sql """
    ALTER TABLE ${tableName} rename ${newTableName};
    """
    order_qt_rename_table "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
    ALTER TABLE ${newTableName} rename ${tableName};
    """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")
    // drop table
    sql """
        drop table ${tableName}
    """
    order_qt_drop_table "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO ${tableName} VALUES("2022-10-26",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinished(jobName)
    order_qt_success "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    mv_rewrite_success_without_check_chosen("""${querySql}""", "${mvName}")

    qt_desc_mv_1 "desc ${mvName}"

    sql """ DROP MATERIALIZED VIEW ${mvName}"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}(event_Day,Id,UserName)
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        ${querySql};
    """
    qt_desc_mv_2 "desc ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${newTableName}`"""
    sql """ DROP MATERIALIZED VIEW ${mvName}"""
}
