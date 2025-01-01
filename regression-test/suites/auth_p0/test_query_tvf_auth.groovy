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

suite("test_jdbc_query_tvf","p0,auth") {
    String suiteName = "test_jdbc_query_tvf"
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String user = "test_jdbc_user";
        String pwd = '123456';
        String catalog_name = "${suiteName}_catalog"
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        String dorisuser = "${suiteName}_user"
        String dorispwd = 'C123_567p'
        try_sql("DROP USER ${dorisuser}")
        sql """CREATE USER '${dorisuser}' IDENTIFIED BY '${dorispwd}'"""
        //cloud-mode
        if (isCloudMode()) {
            def clusters = sql " SHOW CLUSTERS; "
            assertTrue(!clusters.isEmpty())
            def validCluster = clusters[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${dorisuser}""";
        }

        sql """grant select_priv on regression_test to ${dorisuser}"""

        connect(user=dorisuser, password="${dorispwd}", url=context.config.jdbcUrl) {
            test {
                  sql """
                     select * from query('catalog' = '${catalog_name}', 'query' = 'select * from doris_test.all_types');
                  """
                  exception "denied"
            }
        }
        sql """grant select_priv on ${catalog_name}.*.* to ${dorisuser}"""
        connect(user=dorisuser, password="${dorispwd}", url=context.config.jdbcUrl) {
          sql """
             select * from query('catalog' = '${catalog_name}', 'query' = 'select * from doris_test.all_types');
          """
        }
        try_sql("DROP USER ${dorisuser}")
        sql """drop catalog if exists ${catalog_name} """
    }
}

