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

suite("test_property_session") {
    String suiteName = "test_property_session"
    String userName = "${suiteName}_user"
    String pwd = 'C123_567p'
    sql """drop user if exists ${userName}"""
    sql """CREATE USER '${userName}' IDENTIFIED BY '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${userName}""";
    }
    sql """GRANT select_PRIV ON *.*.* TO ${userName}""";
    connect(userName, "${pwd}", context.config.jdbcUrl) {
        sql """
              set query_timeout=1;
          """
        test {
              sql """
                  select sleep(3);
              """
              exception "timeout"
        }
    }

    // the priority of property should be higher than session
    sql """set property for '${userName}' 'query_timeout' = '10';"""
    connect(userName, "${pwd}", context.config.jdbcUrl) {
        sql """
            select sleep(3);
        """
    }

    sql """drop user if exists ${userName}"""
}
