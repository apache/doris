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

suite("test_use_encryptkey_auth","p0,auth") {
    multi_sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
    """
    String suiteName = "test_version_info_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String user = "${suiteName}_user"
    String key = "${suiteName}_key"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    try_sql("DROP ENCRYPTKEY ${key}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }
    sql """CREATE ENCRYPTKEY ${key} AS 'ABCD123456789'"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
              sql """
                  SELECT HEX(AES_ENCRYPT("Doris is Great", KEY ${dbName}.${key}));
              """
              exception "denied"
          }
    }
    sql """grant select_priv on ${dbName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
          sql """
              SELECT HEX(AES_ENCRYPT("Doris is Great", KEY ${dbName}.${key}));
          """
    }
    try_sql("DROP USER ${user}")
    try_sql("DROP ENCRYPTKEY ${key}")
}
