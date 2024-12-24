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

suite("test_partition_values_tvf_auth","p0,auth") {
    String suiteName = "test_partition_values_tvf_auth"
     String enabled = context.config.otherConfigs.get("enableHiveTest")
     if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
     }

     for (String hivePrefix : ["hive3"]) {
             String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
             String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
             String catalog_name = "${hivePrefix}_test_external_catalog_hive_partition"

             sql """drop catalog if exists ${catalog_name};"""
             sql """
                 create catalog if not exists ${catalog_name} properties (
                     'type'='hms',
                     'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
                 );
             """
            String user = "${suiteName}_user"
            String pwd = 'C123_567p'
            try_sql("DROP USER ${user}")
            sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
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
                          select * from partition_values("catalog" = "${catalog_name}", "database" = "multi_catalog", "table" = "orc_partitioned_columns") order by t_int, t_float;
                       """
                       exception "denied"
                 }
             }
            sql """grant select_priv on ${catalog_name}.multi_catalog.orc_partitioned_columns to ${user}"""
            connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
               sql """
                  select * from partition_values("catalog" = "${catalog_name}", "database" = "multi_catalog", "table" = "orc_partitioned_columns") order by t_int, t_float;
               """
             }
             try_sql("DROP USER ${user}")
             sql """drop catalog if exists ${catalog_name}"""
         }
}

