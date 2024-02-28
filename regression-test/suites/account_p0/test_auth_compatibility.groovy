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

suite("test_auth_compatibility", "account") {
    def user = 'acount_auth_compatibility_user'
    def pwd = 'C123_567p'
    def dbName = 'account_auth_compatibility_db'
    def tableName = 'account_auth_compatibility_table'
    def viewName = 'account_auth_compatibility_table_view'

    def getProperty = { property, userName ->
        def result = null
        if (userName == "") {
            result = sql_return_maparray """SHOW PROPERTY""" 
        } else {
            result = sql_return_maparray """SHOW PROPERTY FOR '${userName}'""" 
        }
        result.find {
            it.Key == property as String
        }
    }

    // not drop user, db, table for test compatibility
    sql """CREATE USER IF NOT EXISTS '${user}'"""
    sql """SET PASSWORD FOR '${user}' = PASSWORD('${pwd}')"""
    sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
    sql """USE ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${tableName}
        (
        k1 TINYINT,
        k2 DECIMAL(10, 2) DEFAULT "10.5",
        v1 HLL HLL_UNION,
        v2 BITMAP BITMAP_UNION
        )
        ENGINE=olap
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        CREATE VIEW IF NOT EXISTS ${dbName}.${viewName} (k1, k2) AS SELECT k1, k2  FROM ${dbName}.${tableName} WHERE k1 = 20160112 GROUP BY k1,k2;
    """
    sql """
        GRANT SELECT_PRIV on *.*.* to ${user};
    """

    sql """
        GRANT SHOW_VIEW_PRIV on ${dbName}.${viewName} TO ${user}
    """

    connect(user = user, password = pwd, url = context.config.jdbcUrl) {
        sql """USE ${dbName}"""
        // auth ok, no exception
        sql """SHOW CREATE TABLE ${viewName}"""
    }

    sql """SET PROPERTY FOR '${user}' 'max_user_connections'= '2048'"""

    sql """SET PROPERTY FOR '${user}'
    'load_cluster.cluster1.hadoop_palo_path' = '/user/doris/doris_path',
    'load_cluster.cluster1.hadoop_configs' = 'fs.default.name=hdfs://dpp.cluster.com:port;mapred.job.tracker=dpp.cluster.com:port;hadoop.job.ugi=user,password;mapred.job.queue.name=job_queue_name_in_hadoop;mapred.job.priority=HIGH;';
    """
    sql """SET PROPERTY FOR '${user}' 'default_load_cluster' = 'cluster1'"""

    def result = getProperty("max_user_connections", "${user}")
    assertEquals(result.Value as String, "2048" as String)

    result = getProperty("default_load_cluster", "${user}")
    assertEquals(result.Value as String, "cluster1" as String)

    result = getProperty("load_cluster.cluster1.hadoop_palo_path", "${user}")
    assertEquals(result.Value as String, "/user/doris/doris_path" as String)
}
