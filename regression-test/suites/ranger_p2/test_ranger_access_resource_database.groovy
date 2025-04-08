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


suite("test_ranger_access_resource_database", "p2,ranger,external") {
	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"
	def checkDatabaseAccess = { catalogType, access, user, password, catalog, dbName, tableName ->
		connect("$user", "$password", "$defaultJdbcUrl") {
			def executeSqlWithLogging = { sqlStatement, errorMessage ->
				try {
					sql sqlStatement
				} catch (Exception e) {
					if (access == "allow") {
						log.error("Error executing ${sqlStatement}: ${e.getMessage()}")
						throw e
					}
					log.info("Error executing ${sqlStatement}: ${e.getMessage()}")
				}
			}
			if (catalogType == "internal") {
				executeSqlWithLogging("""SWITCH ${catalog}""", "Error executing SWITCH")
				executeSqlWithLogging("""
				    CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
				        id BIGINT,
				        username VARCHAR(20)
				    )
				    DISTRIBUTED BY HASH(id) BUCKETS 2
				    PROPERTIES (
				        "replication_num" = "1"
				    );
				""", "Error executing CREATE TABLE")
				executeSqlWithLogging("""INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')""", "Error executing INSERT")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.${tableName}""", "Error executing SELECT")
				executeSqlWithLogging("""ALTER TABLE ${dbName}.${tableName} ADD COLUMN age INT""", "Error executing ALTER TABLE")
				executeSqlWithLogging("""DROP VIEW IF EXISTS ${dbName}.test_view""", "Error executing DROP VIEW")
				executeSqlWithLogging("""CREATE VIEW ${dbName}.test_view AS SELECT * FROM ${dbName}.${tableName}""", "Error executing CREATE VIEW")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.test_view""", "Error executing SELECT VIEW")
				executeSqlWithLogging("""SHOW CREATE VIEW ${dbName}.test_view""", "Error executing SHOW CREATE VIEW")
				executeSqlWithLogging("""DROP TABLE ${dbName}.${tableName}""", "Error executing DROP DATABASE")
			} else if (catalogType == "hive") {
				executeSqlWithLogging("""SWITCH ${catalog}""", "Error executing SWITCH")
				executeSqlWithLogging("""
				    CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
				      id BIGINT,
				      username VARCHAR(20)
					)  ENGINE=hive
					PROPERTIES (
					  'file_format'='parquet'
					);
				""", "Error executing CREATE TABLE")
				executeSqlWithLogging("""INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')""", "Error executing INSERT")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.${tableName}""", "Error executing SELECT")
				executeSqlWithLogging("""DROP TABLE ${dbName}.${tableName}""", "Error executing DROP DATABASE")
			}
		}
	}

	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")
	String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
	String HmsPort = context.config.otherConfigs.get("hive3HmsPort")

	if (enabled != null && enabled.equalsIgnoreCase("true")) {
		String catalog1 = 'ranger_catalog_3'
		// prepare catalog
		sql """DROP CATALOG IF EXISTS ${catalog1}"""
		sql """CREATE CATALOG `${catalog1}` PROPERTIES (
			"type"="hms",
			'hive.metastore.uris' = 'thrift://${externalEnvIp}:${HmsPort}'
		)"""

		// prepare database
		List<String> catalogDbList = ['ranger_test_catalog3_db_1', 'ranger_test_catalog3_db_2']
		List<String> internalDbList = ['ranger_test_internal_db_1', 'ranger_test_internal_db_2']
		catalogDbList.forEach {
			sql """DROP DATABASE IF EXISTS ${catalog1}.${it}"""
			sql """CREATE DATABASE IF NOT EXISTS ${catalog1}.${it}"""
		}
		internalDbList.forEach {
			sql """DROP DATABASE IF EXISTS ${it}"""
			sql """CREATE DATABASE IF NOT EXISTS ${it}"""
		}

		// prepare user
		List<String> userList = ['ranger_test_db_user_1', 'ranger_test_db_user_2', 'ranger_test_db_user_3',
		                         'ranger_test_db_user_4', 'ranger_test_db_user_5']
		String pwd = 'C123_567p'
		userList.forEach {
			sql """DROP USER IF EXISTS ${it}"""
			sql """CREATE USER '${it}' IDENTIFIED BY '${pwd}'"""
		}
		// case1
		// create policy
		RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
		String policy1 = 'ranger_test_db_policy_1'
		List<String> dbPolicy = ["GRANT", "SELECT", "LOAD", "ALTER", "CREATE", "DROP", "SHOW_VIEW"]

		Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource("internal"))
		resource.put("database", new RangerPolicy.RangerPolicyResource(internalDbList[0]))
		RangerPolicy policy = new RangerPolicy()

		policy.setService(rangerServiceName)
		policy.setName(policy1)
		policy.setResources(resource)
		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem()
		policyItem.setUsers([userList[0]])

		List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>()
		dbPolicy.forEach {
			policyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess(it))
		}
		policyItem.setAccesses(policyItemAccesses)
		policy.setPolicyItems([policyItem])
		try {
			rangerClient.deletePolicy(rangerServiceName, policy1)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
		RangerPolicy createdPolicy = rangerClient.createPolicy(policy)
		println("New Policy created with id: " + createdPolicy.getId())
		// sleep 6s to wait for ranger policy to take effect
		// ranger.plugin.doris.policy.pollIntervalMs is 5000ms in ranger-doris-security.xml
		waitPolicyEffect()
		checkDatabaseAccess("internal", "allow", userList[0], pwd, "internal", internalDbList[0], 'ranger_test_internal_table_1')
		checkDatabaseAccess("internal", "deny", userList[0], pwd, "internal", internalDbList[1], 'ranger_test_internal_table_2')
		rangerClient.deletePolicy(rangerServiceName, policy1)

		// case2
		String policy2 = 'ranger_test_db_policy_2'
		policy.setName(policy2)
		resource.clear()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource("internal"))
		resource.put("database", new RangerPolicy.RangerPolicyResource("*"))
		policy.setResources(resource)
		policyItem.setUsers([userList[1]])
		try {
			rangerClient.deletePolicy(rangerServiceName, policy2)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
		createdPolicy = rangerClient.createPolicy(policy)
		println("New Policy created with id: " + createdPolicy.getId())
		System.out.println("New Policy created with id: " + createdPolicy.getId())
		waitPolicyEffect()
		checkDatabaseAccess("internal", "allow", userList[1], pwd, "internal", internalDbList[0], 'ranger_test_internal_table_1')
		checkDatabaseAccess("internal", "allow", userList[1], pwd, "internal", internalDbList[1], 'ranger_test_internal_table_2')
		checkDatabaseAccess("hive", "deny", userList[1], pwd, catalog1, catalogDbList[0], 'ranger_test_catalog_table_1')
		rangerClient.deletePolicy(rangerServiceName, policy2)


		// case3
		String policy3 = 'ranger_test_db_policy_3'
		policy.setName(policy3)
		resource.clear()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource(catalog1))
		resource.put("database", new RangerPolicy.RangerPolicyResource(catalogDbList[0]))
		policy.setResources(resource)
		policyItem.setUsers([userList[2]])
		try {
			rangerClient.deletePolicy(rangerServiceName, policy3)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
		createdPolicy = rangerClient.createPolicy(policy)
		println("New Policy created with id: " + createdPolicy.getId())
		waitPolicyEffect()
		checkDatabaseAccess("hive", "allow", userList[2], pwd, catalog1, catalogDbList[0], 'ranger_test_catalog_table_1')
		checkDatabaseAccess("hive", "deny", userList[2], pwd, catalog1, catalogDbList[1], 'ranger_test_catalog_table_2')
		rangerClient.deletePolicy(rangerServiceName, policy3)

		// case4
		String policy4 = 'ranger_test_db_policy_4'
		policy.setName(policy4)
		resource.clear()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource(catalog1))
		resource.put("database", new RangerPolicy.RangerPolicyResource("*"))
		policy.setResources(resource)
		policyItem.setUsers([userList[3]])
		try {
			rangerClient.deletePolicy(rangerServiceName, policy4)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
		createdPolicy = rangerClient.createPolicy(policy)
		println("New Policy created with id: " + createdPolicy.getId())
		waitPolicyEffect()
		checkDatabaseAccess("hive", "allow", userList[3], pwd, catalog1, catalogDbList[0], 'ranger_test_catalog_table_1')
		checkDatabaseAccess("hive", "allow", userList[3], pwd, catalog1, catalogDbList[1], 'ranger_test_catalog_table_2')
		checkDatabaseAccess("internal", "deny", userList[3], pwd, "internal", internalDbList[0], 'ranger_test_internal_table_1')
		checkDatabaseAccess("internal", "deny", userList[3], pwd, "internal", internalDbList[1], 'ranger_test_internal_table_2')
		rangerClient.deletePolicy(rangerServiceName, policy4)

		// case5
		String policy5 = 'ranger_test_db_policy_5'
		policy.setName(policy5)
		resource.clear()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource("*"))
		resource.put("database", new RangerPolicy.RangerPolicyResource([catalogDbList[1], internalDbList[1]], false, false))
		policy.setResources(resource)
		policyItem.setUsers([userList[4]])
		try {
			rangerClient.deletePolicy(rangerServiceName, policy5)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
		createdPolicy = rangerClient.createPolicy(policy)
		println("New Policy created with id: " + createdPolicy.getId())
		waitPolicyEffect()
		checkDatabaseAccess("hive", "deny", userList[4], pwd, catalog1, catalogDbList[0], 'ranger_test_catalog_table_1')
		checkDatabaseAccess("hive", "allow", userList[4], pwd, catalog1, catalogDbList[1], 'ranger_test_catalog_table_2')
		checkDatabaseAccess("internal", "deny", userList[4], pwd, "internal", internalDbList[0], 'ranger_test_internal_table_1')
		checkDatabaseAccess("internal", "allow", userList[4], pwd, "internal", internalDbList[1], 'ranger_test_internal_table_2')
		rangerClient.deletePolicy(rangerServiceName, policy5)

	}
}