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

suite("test_catalogs_auth","p0,auth") {
    String suiteName = "test_catalogs_auth"
    String catalogName = "${suiteName}_catalog"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

   sql """drop catalog if exists ${catalogName}"""
    sql """CREATE CATALOG ${catalogName} PROPERTIES (
            "type"="es",
            "hosts"="http://8.8.8.8:9200"
        );"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def showRes = sql """show catalogs;"""
        logger.info("showRes: " + showRes.toString())
        assertFalse(showRes.toString().contains("${catalogName}"))

        def tvfRes = sql """select * from catalogs();"""
        logger.info("tvfRes: " + tvfRes.toString())
        assertFalse(tvfRes.toString().contains("${catalogName}"))
    }

    sql """grant select_priv on ${catalogName}.*.* to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def showRes = sql """show catalogs;"""
        logger.info("showRes: " + showRes.toString())
        assertTrue(showRes.toString().contains("${catalogName}"))

        def tvfRes = sql """select * from catalogs();"""
        logger.info("tvfRes: " + tvfRes.toString())
        assertTrue(tvfRes.toString().contains("${catalogName}"))
    }

    try_sql("DROP USER ${user}")
    sql """drop catalog if exists ${catalogName}"""
}
