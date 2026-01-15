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

suite("test_jdbc_catalog_refresh_update_time", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    // Helper function to get LastUpdateTime for a catalog
    // Column index: 0=CatalogId, 1=Name, 2=Type, 3=IsCurrent, 4=CreateTime, 5=LastUpdateTime
    def getLastUpdateTime = { catalogName ->
        def catalogs = sql """show catalogs"""
        for (row in catalogs) {
            if (row[1] == catalogName) {
                return row[5]
            }
        }
        return null
    }

    String mysql_port = context.config.otherConfigs.get("mysql_57_port")
    String catalog_name = "test_refresh_update_time_catalog"

    sql """drop catalog if exists ${catalog_name}"""

    // Test 1: Manual refresh should update LastUpdateTime
    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    );"""

    // Get initial LastUpdateTime
    String initialUpdateTime = getLastUpdateTime(catalog_name)
    assertNotNull(initialUpdateTime, "Catalog ${catalog_name} should exist")

    // Wait a bit to ensure time difference
    Thread.sleep(2000)

    // Manual refresh
    sql """refresh catalog ${catalog_name}"""

    // Get LastUpdateTime after refresh
    String afterRefreshUpdateTime = getLastUpdateTime(catalog_name)

    // Verify LastUpdateTime changed after manual refresh
    assertTrue(afterRefreshUpdateTime != initialUpdateTime,
        "LastUpdateTime should change after manual refresh. Initial: ${initialUpdateTime}, After: ${afterRefreshUpdateTime}")

    sql """drop catalog if exists ${catalog_name}"""

    // Test 2: Scheduled refresh should update LastUpdateTime
    String scheduled_catalog_name = "test_scheduled_refresh_catalog"
    sql """drop catalog if exists ${scheduled_catalog_name}"""

    sql """create catalog if not exists ${scheduled_catalog_name} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver",
        "metadata_refresh_interval_sec" = "5"
    );"""

    // Get initial LastUpdateTime
    String scheduledInitialTime = getLastUpdateTime(scheduled_catalog_name)
    assertNotNull(scheduledInitialTime, "Catalog ${scheduled_catalog_name} should exist")

    // Wait for scheduled refresh (interval is 5 seconds, wait 8 seconds to be safe)
    Thread.sleep(8000)

    // Get LastUpdateTime after scheduled refresh
    String scheduledAfterRefreshTime = getLastUpdateTime(scheduled_catalog_name)

    // Verify LastUpdateTime changed after scheduled refresh
    assertTrue(scheduledAfterRefreshTime != scheduledInitialTime,
        "LastUpdateTime should change after scheduled refresh. Initial: ${scheduledInitialTime}, After: ${scheduledAfterRefreshTime}")

    sql """drop catalog if exists ${scheduled_catalog_name}"""
}
