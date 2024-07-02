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

import org.junit.Assert;

suite("test_session_variable_mtmv","mtmv") {
    String suiteName = "test_session_variable_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    def dbName = "regression_test_mtmv_p0"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

     sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """

    test {
          sql """
              CREATE MATERIALIZED VIEW ${mvName}
              BUILD DEFERRED REFRESH AUTO ON MANUAL
              DISTRIBUTED BY RANDOM BUCKETS 2
              PROPERTIES (
              'replication_num' = '1',
              'session.gggg' = 'fff'
              )
              AS
              SELECT * from ${tableName};
              """
          exception "Unknown system variable"
      }

    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1',
            'session.query_timeout' = 'fff'
            )
            AS
            SELECT * from ${tableName};
            """
        exception "string"
    }

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'session.query_timeout' = '3700'
        )
        AS
        SELECT * from ${tableName};
        """

    test {
        sql """
            alter MATERIALIZED VIEW ${mvName} set("session.query_timeout"="gg");
        """
        exception "string"
       }

    sql """
        alter MATERIALIZED VIEW ${mvName} set("session.query_timeout"="3800");
    """

     // refresh mv
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mv "SELECT * FROM ${mvName}"

    sql """drop materialized view if exists ${mvName};"""


     sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'session.workload_group' = 'group_not_exist'
        )
        AS
        SELECT * from ${tableName};
        """
     // refresh mv
      sql """
         REFRESH MATERIALIZED VIEW ${mvName} complete
         """
     def jobName = getJobName(dbName, mvName);
     waitingMTMVTaskFinishedNotNeedSuccess(jobName)
     order_qt_refresh_group "select Status,ErrorMsg from tasks('type'='mv') where MvName = '${mvName}' order by CreateTime DESC limit 1"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
