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

suite("test_ranger_access_resource_global", "p2,ranger,external") {
	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"
	def checkGlobalAccess = { catalogType, access, user, password, catalog, dbName, tableName ->
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
				executeSqlWithLogging("""DROP DATABASE IF EXISTS ${dbName}""", "Error executing DROP DATABASE")
				executeSqlWithLogging("""CREATE DATABASE IF NOT EXISTS ${dbName}""", "Error executing CREATE DATABASE")
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
				executeSqlWithLogging("""CREATE VIEW ${dbName}.test_view AS SELECT * FROM ${dbName}.${tableName}""", "Error executing CREATE VIEW")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.test_view""", "Error executing SELECT VIEW")
				executeSqlWithLogging("""SHOW CREATE VIEW ${dbName}.test_view""", "Error executing SHOW CREATE VIEW")
				executeSqlWithLogging("""DROP TABLE IF EXISTS ${dbName}.${tableName}""", "Error executing DROP TABLE")
				executeSqlWithLogging("""DROP DATABASE ${dbName}""", "Error executing DROP DATABASE")
			} else if (catalogType == "hive") {
				executeSqlWithLogging("""SWITCH ${catalog}""", "Error executing SWITCH")
				executeSqlWithLogging("""CREATE DATABASE IF NOT EXISTS ${dbName}""", "Error executing CREATE DATABASE")
				executeSqlWithLogging("""DROP TABLE IF EXISTS ${dbName}.${tableName}""", "Error executing DROP TABLE")
				executeSqlWithLogging("""
				    CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
				      id BIGINT,
				      username VARCHAR(20)
					)  ENGINE=hive
					PROPERTIES (
					  'file_format'='parquet'
					);
				""", "Error executing CREATE TABLE")
				executeSqlWithLogging("""REFRESH CATALOG ${catalog}""", "Error executing REFRESH")
				executeSqlWithLogging("""INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')""", "Error executing INSERT")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.${tableName}""", "Error executing SELECT")
				executeSqlWithLogging("""DROP TABLE IF EXISTS ${dbName}.${tableName}""", "Error executing DROP TABLE")
				executeSqlWithLogging("""DROP DATABASE ${dbName}""", "Error executing DROP DATABASE")
			} else if (catalogType == "jdbc") {
				executeSqlWithLogging("""SWITCH ${catalog}""", "Error executing SWITCH")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'CREATE DATABASE IF NOT EXISTS ${dbName}')""", "Error executing CREATE DATABASE")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'DROP TABLE IF EXISTS ${dbName}.${tableName}')""", "Error executing DROP TABLE")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (id BIGINT, username VARCHAR(20))')""", "Error executing CREATE TABLE")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'INSERT INTO ${dbName}.${tableName} VALUES (1, ''test'')')""", "Error executing INSERT")
				executeSqlWithLogging("""SELECT * FROM ${dbName}.${tableName}""", "Error executing SELECT")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'ALTER TABLE ${dbName}.${tableName} ADD COLUMN age INT')""", "Error executing ALTER TABLE")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'DROP TABLE IF EXISTS ${dbName}.${tableName}')""", "Error executing DROP TABLE")
				executeSqlWithLogging("""CALL EXECUTE_STMT ('${catalog}', 'DROP DATABASE ${dbName}')""", "Error executing DROP DATABASE")
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
	String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
	String jdbcUser = context.config.jdbcUser
	String jdbcPassword = context.config.jdbcPassword
	String s3Endpoint = getS3Endpoint()
	String bucket = getS3BucketName()
	String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

	if (enabled != null && enabled.equalsIgnoreCase("true")) {
		String catalog1 = 'ranger_test_global_1'
		String catalog2 = 'ranger_test_global_2'
		// prepare catalog
		sql """DROP CATALOG IF EXISTS ${catalog1}"""
		sql """DROP CATALOG IF EXISTS ${catalog2}"""
		sql """CREATE CATALOG `${catalog1}` PROPERTIES (
			"type"="hms",
			'hive.metastore.uris' = 'thrift://${externalEnvIp}:${HmsPort}'
		)"""

		sql """ CREATE CATALOG `${catalog2}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driverUrl}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""
		// prepare doris user
		List<String> userList = ['ranger_test_global_user_1']
		String pwd = 'C123_567p'
		userList.forEach {
			sql """DROP USER IF EXISTS ${it}"""
			sql """CREATE USER '${it}' IDENTIFIED BY '${pwd}'"""
		}


		// case1
		// create policy
		RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
		String policy1 = 'all - global'
		List<String> globalPolicy = ["GRANT", "SELECT", "LOAD", "ALTER", "CREATE", "DROP", "SHOW_VIEW", "ADMIN", "NODE"]

		Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>()
		resource.put("global", new RangerPolicy.RangerPolicyResource("*"))
		RangerPolicy policy = new RangerPolicy()


		policy.setService(rangerServiceName)
		policy.setName(policy1)
		policy.setResources(resource)
		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem()
		policyItem.setUsers([userList[0], "admin", "root"])
		List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>()
		globalPolicy.forEach {
			policyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess(it))
		}
		policyItem.setAccesses(policyItemAccesses)
		policy.setPolicyItems([policyItem])
		rangerClient.updatePolicy(rangerServiceName, "all%20-%20global", policy)


		// sleep 6s to wait for ranger policy to take effect
		// ranger.plugin.doris.policy.pollIntervalMs is 5000ms in ranger-doris-security.xml
		waitPolicyEffect()
		checkGlobalAccess("internal", "allow", userList[0], pwd, "internal", 'ranger_test_global_db_1', 'ranger_test_global_table_1')
		checkGlobalAccess("hive", "allow", userList[0], pwd, catalog1, 'ranger_test_global_db_2', 'ranger_test_global_table_2')
		checkGlobalAccess("jdbc", "allow", userList[0], pwd, catalog2, 'ranger_test_global_db_3', 'ranger_test_global_table_3')
	}
}