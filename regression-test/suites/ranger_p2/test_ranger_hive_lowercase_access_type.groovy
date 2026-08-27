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

import org.apache.ranger.RangerClient
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.model.RangerService


suite("test_ranger_hive_lowercase_access_type", "p2,ranger,external") {
    String enableRangerTest = context.config.otherConfigs.get("enableRangerTest")
    String enableHiveTest = context.config.otherConfigs.get("enableHiveTest")

    if (!enableRangerTest?.equalsIgnoreCase("true") || !enableHiveTest?.equalsIgnoreCase("true")) {
        logger.info("skip Ranger-Hive lowercase access type case because Ranger or Hive test is not enabled")
        return
    }

    String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
    String rangerUser = context.config.otherConfigs.get("rangerUser")
    String rangerPassword = context.config.otherConfigs.get("rangerPassword")
    String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String hiveServerPort = context.config.otherConfigs.get("hive3ServerPort")
    String rangerHiveServiceName = "${rangerServiceName}_hive"

    String catalog = "ranger_hive_lowercase_catalog"
    String cleanupCatalog = "ranger_hive_lowercase_cleanup_catalog"
    String database = "ranger_hive_lowercase_db"
    String table = "ranger_hive_lowercase_tbl"
    String user = "ranger_hive_lowercase_user"
    String password = "C123_567p"
    String accessPolicyName = "doris_ranger_hive_lowercase_access"
    String rowFilterPolicyName = "doris_ranger_hive_lowercase_row_filter"
    String dataMaskPolicyName = "doris_ranger_hive_lowercase_data_mask"
    List<String> policyNames = [accessPolicyName, rowFilterPolicyName, dataMaskPolicyName]

    RangerClient rangerClient = new RangerClient(
            "http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
    boolean createdHiveService = false

    def deletePolicyIfExists = { String policyName ->
        try {
            rangerClient.deletePolicy(rangerHiveServiceName, policyName)
        } catch (Exception e) {
            logger.info("Ranger policy ${policyName} does not exist: ${e.getMessage()}")
        }
    }

    def tableResources = {
        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>()
        resources.put("database", new RangerPolicy.RangerPolicyResource(database))
        resources.put("table", new RangerPolicy.RangerPolicyResource(table))
        return resources
    }

    def accessResources = {
        Map<String, RangerPolicy.RangerPolicyResource> resources = tableResources()
        resources.put("column", new RangerPolicy.RangerPolicyResource("*"))
        return resources
    }

    try {
        try {
            RangerService hiveService = rangerClient.getService(rangerHiveServiceName)
            assertEquals("hive", hiveService.getType())
        } catch (Exception e) {
            logger.info("Create Ranger-Hive service ${rangerHiveServiceName}: ${e.getMessage()}")
            RangerService hiveService = new RangerService()
            hiveService.setType("hive")
            hiveService.setName(rangerHiveServiceName)
            hiveService.setDisplayName(rangerHiveServiceName)
            hiveService.setConfigs([
                    "username": "hive",
                    "password": "hive",
                    "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
                    "jdbc.url": "jdbc:hive2://${externalEnvIp}:${hiveServerPort}".toString()
            ])
            rangerClient.createService(hiveService)
            createdHiveService = true
        }

        // Prepare the Hive data through a catalog without Ranger, so setup and cleanup do not need policies.
        sql """DROP CATALOG IF EXISTS `${catalog}`"""
        sql """DROP CATALOG IF EXISTS `${cleanupCatalog}`"""
        sql """CREATE CATALOG `${catalog}` PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
        )"""
        try_sql """DROP TABLE IF EXISTS `${catalog}`.`${database}`.`${table}`"""
        try_sql """DROP DATABASE IF EXISTS `${catalog}`.`${database}`"""
        sql """CREATE DATABASE `${catalog}`.`${database}`"""
        sql """CREATE TABLE `${catalog}`.`${database}`.`${table}` (
                id BIGINT,
                secret VARCHAR(20)
        ) ENGINE=hive
        PROPERTIES ('file_format' = 'parquet')"""
        sql """INSERT INTO `${catalog}`.`${database}`.`${table}` VALUES
                (1, 'first'), (2, 'second')"""
        sql """DROP CATALOG `${catalog}`"""

        sql """DROP USER IF EXISTS '${user}'"""
        sql """CREATE USER '${user}' IDENTIFIED BY '${password}'"""

        policyNames.each { deletePolicyIfExists(it) }

        RangerPolicy accessPolicy = new RangerPolicy()
        accessPolicy.setService(rangerHiveServiceName)
        accessPolicy.setName(accessPolicyName)
        accessPolicy.setResources(accessResources())
        RangerPolicy.RangerPolicyItem accessPolicyItem = new RangerPolicy.RangerPolicyItem()
        accessPolicyItem.setUsers([user])
        accessPolicyItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
        accessPolicy.setPolicyItems([accessPolicyItem])
        rangerClient.createPolicy(accessPolicy)

        RangerPolicy rowFilterPolicy = new RangerPolicy()
        rowFilterPolicy.setService(rangerHiveServiceName)
        rowFilterPolicy.setName(rowFilterPolicyName)
        rowFilterPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER)
        rowFilterPolicy.setResources(tableResources())
        RangerPolicy.RangerRowFilterPolicyItem rowFilterPolicyItem = new RangerPolicy.RangerRowFilterPolicyItem()
        rowFilterPolicyItem.setUsers([user])
        rowFilterPolicyItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
        rowFilterPolicyItem.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id >= 2"))
        rowFilterPolicy.setRowFilterPolicyItems([rowFilterPolicyItem])
        rangerClient.createPolicy(rowFilterPolicy)

        RangerPolicy dataMaskPolicy = new RangerPolicy()
        dataMaskPolicy.setService(rangerHiveServiceName)
        dataMaskPolicy.setName(dataMaskPolicyName)
        dataMaskPolicy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK)
        Map<String, RangerPolicy.RangerPolicyResource> dataMaskResources = tableResources()
        dataMaskResources.put("column", new RangerPolicy.RangerPolicyResource("secret"))
        dataMaskPolicy.setResources(dataMaskResources)
        RangerPolicy.RangerDataMaskPolicyItem dataMaskPolicyItem = new RangerPolicy.RangerDataMaskPolicyItem()
        dataMaskPolicyItem.setUsers([user])
        dataMaskPolicyItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
        dataMaskPolicyItem.setDataMaskInfo(
                new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK_NULL", "", ""))
        dataMaskPolicy.setDataMaskPolicyItems([dataMaskPolicyItem])
        rangerClient.createPolicy(dataMaskPolicy)

        // Recreate the catalog with Ranger-Hive enabled after policies are ready.
        sql """CREATE CATALOG `${catalog}` PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'access_controller.properties.ranger.service.name' = '${rangerHiveServiceName}',
                'access_controller.class' =
                        'org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory'
        )"""
        waitPolicyEffect()

        def tokens = context.config.jdbcUrl.split('/')
        def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"
        connect(user, password, defaultJdbcUrl) {
            String query = "SELECT id, secret FROM `${catalog}`.`${database}`.`${table}` ORDER BY id"

            // The first query initializes Ranger's optimized policy evaluator. The second query verifies
            // that lowercase Hive access types still match after that evaluator has been warmed up.
            order_qt_lowercase_access_type_first(query)
            order_qt_lowercase_access_type_warmed_up(query)
        }
    } finally {
        try_sql "DROP CATALOG IF EXISTS `${catalog}`"
        policyNames.each { deletePolicyIfExists(it) }
        try_sql "DROP USER IF EXISTS '${user}'"
        if (createdHiveService) {
            try {
                rangerClient.deleteService(rangerHiveServiceName)
            } catch (Exception e) {
                logger.warn("Failed to delete Ranger-Hive service ${rangerHiveServiceName}: ${e.getMessage()}")
            }
        }

        try {
            sql """DROP CATALOG IF EXISTS `${cleanupCatalog}`"""
            sql """CREATE CATALOG `${cleanupCatalog}` PROPERTIES (
                    'type' = 'hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            )"""
            try_sql """DROP TABLE IF EXISTS `${cleanupCatalog}`.`${database}`.`${table}`"""
            try_sql """DROP DATABASE IF EXISTS `${cleanupCatalog}`.`${database}`"""
        } finally {
            try_sql "DROP CATALOG IF EXISTS `${cleanupCatalog}`"
        }
    }
}
