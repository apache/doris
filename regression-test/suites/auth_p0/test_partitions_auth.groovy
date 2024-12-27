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

suite("test_partitions_auth","p0,auth") {
    String suiteName = "test_partitions_auth"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

   sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
              sql """
                 show partitions from ${dbName}.${tableName};
              """
              exception "denied"
        }
        test {
              sql """
                 select * from partitions('catalog'='internal',"database"="${dbName}","table"="${tableName}");
              """
              exception "denied"
        }
    }

    sql """grant select_priv on ${dbName}.${tableName} to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """
             show partitions from ${dbName}.${tableName};
          """
        sql """
             select * from partitions('catalog'='internal',"database"="${dbName}","table"="${tableName}");
          """
    }

    try_sql("DROP USER ${user}")
    sql """drop table if exists `${tableName}`"""
}
