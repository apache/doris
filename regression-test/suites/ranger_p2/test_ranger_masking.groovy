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


suite("test_ranger_masking", "p2,ranger,external") {
	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String enableHiveTest = context.config.otherConfigs.get("enableHiveTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")
	String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
	String HmsPort = context.config.otherConfigs.get("hive3HmsPort")

	if ((enabled != null && enabled.equalsIgnoreCase("true")) && (enableHiveTest != null && enableHiveTest.equalsIgnoreCase("true"))) {
		String catalog1 = 'ranger_catalog_6'
		// prepare catalog
		sql """DROP CATALOG IF EXISTS ${catalog1}"""
		sql """CREATE CATALOG `${catalog1}` PROPERTIES (
			"type"="hms",
			'hive.metastore.uris' = 'thrift://${externalEnvIp}:${HmsPort}'
		)"""
		// prepare database
		String catalogDb = 'ranger_catalog_6_db_1'
		sql """CREATE DATABASE IF NOT EXISTS ${catalog1}.${catalogDb}"""

		String internalDb = 'ranger_internal_db_5'
		sql """CREATE DATABASE IF NOT EXISTS ${internalDb}"""

		// prepare table
		String catalogTable = 'ranger_catalog_6_db1_tbl1'
		sql """DROP TABLE IF EXISTS ${catalog1}.${catalogDb}.${catalogTable}"""
		sql """CREATE TABLE IF NOT EXISTS ${catalog1}.${catalogDb}.${catalogTable} (
			id BIGINT,
			c1 VARCHAR(20),
			c2 VARCHAR(20),
			c3 VARCHAR(20),
			c4 VARCHAR(20),
			c5 VARCHAR(20),
			c6 VARCHAR(20),
			c7 VARCHAR(20),
			c8 DATE,
			c9 DATETIME
		)  ENGINE=hive
		PROPERTIES (
			'file_format'='parquet'
		);"""
		String internalTable = 'ranger_internal_db_5_tbl1'
		sql """DROP TABLE IF EXISTS internal.${internalDb}.`${internalTable}`"""
		sql """CREATE TABLE IF NOT EXISTS internal.${internalDb}.`${internalTable}` (
	        id BIGINT,
	        c1 VARCHAR(20),
	        c2 VARCHAR(20),
			c3 VARCHAR(20),
			c4 VARCHAR(20),
			c5 VARCHAR(20),
			c6 VARCHAR(20),
			c7 VARCHAR(20),
			c8 DATE,
			c9 DATETIME
	    )
	    DISTRIBUTED BY HASH(id) BUCKETS 2
	    PROPERTIES (
	        "replication_num" = "1"
	    );"""
		// prepare data
		sql """INSERT INTO ${catalog1}.${catalogDb}.${catalogTable} (id, c1, c2, c3, c4, c5, c6, c7, c8, c9) VALUES
		(1, 'DataOne01', 'SampleA1', 'Value1X', 'InfoX123', 'DescA1', 'Extra1A', 'AddVal1', '2023-01-01', '2010-12-02 19:28:30'),
		(2, 'DataTwo02', 'SampleB2', 'Value2Y', 'InfoY234', 'DescB2', 'Extra2B', 'AddVal2', '2023-02-01', '2010-12-02 19:28:30'),
		(3, 'DataThr03', 'SampleC3', 'Value3Z', 'InfoZ345', 'DescC3', 'Extra3C', 'AddVal3', '2023-03-01', '2010-12-02 19:28:30'),
		(4, 'DataFou04', 'SampleD4', 'Value4W', 'InfoW456', 'DescD4', 'Extra4D', 'AddVal4', '2023-04-01', '2011-12-02 19:28:30'),
		(5, 'DataFiv05', 'SampleE5', 'Value5V', 'InfoV567', 'DescE5', 'Extra5E', 'AddVal5', '2023-05-01', '2012-12-02 19:28:30'),
		(6, 'DataSix06', 'SampleF6', 'Value6U', 'InfoU678', 'DescF6', 'Extra6F', 'AddVal6', '2023-06-01', '2013-12-02 19:28:30'),
		(7, 'DataSev07', 'SampleG7', 'Value7T', 'InfoT789', 'DescG7', 'Extra7G', 'AddVal7', '2023-07-01', '2014-12-02 19:28:30'),
		(8, 'DataEig08', 'SampleH8', 'Value8S', 'InfoS890', 'DescH8', 'Extra8H', 'AddVal8', '2023-08-01', '2015-12-02 19:28:30'),
		(9, 'DataNin09', 'SampleI9', 'Value9R', 'InfoR901', 'DescI9', 'Extra9I', 'AddVal9', '2024-09-01', '2016-12-02 19:28:30'),
		(10, 'DataTen10', 'SampleJ0', 'Value0Q', 'InfoQ012', 'DescJ0', 'Extra0J', 'AddVal0', '2025-10-01', '2017-12-02 19:28:30');"""

		sql """INSERT INTO internal.${internalDb}.${internalTable} (id, c1, c2, c3, c4, c5, c6, c7, c8, c9) VALUES
		(1, 'DataOne01', 'SampleA1', 'Value1X', 'InfoX123', 'DescA1', 'Extra1A', 'AddVal1', '2023-01-01', '2010-12-02 19:28:30'),
		(2, 'DataTwo02', 'SampleB2', 'Value2Y', 'InfoY234', 'DescB2', 'Extra2B', 'AddVal2', '2023-02-01', '2010-12-02 19:28:30'),
		(3, 'DataThr03', 'SampleC3', 'Value3Z', 'InfoZ345', 'DescC3', 'Extra3C', 'AddVal3', '2023-03-01', '2010-12-02 19:28:30'),
		(4, 'DataFou04', 'SampleD4', 'Value4W', 'InfoW456', 'DescD4', 'Extra4D', 'AddVal4', '2023-04-01', '2011-12-02 19:28:30'),
		(5, 'DataFiv05', 'SampleE5', 'Value5V', 'InfoV567', 'DescE5', 'Extra5E', 'AddVal5', '2023-05-01', '2012-12-02 19:28:30'),
		(6, 'DataSix06', 'SampleF6', 'Value6U', 'InfoU678', 'DescF6', 'Extra6F', 'AddVal6', '2023-06-01', '2013-12-02 19:28:30'),
		(7, 'DataSev07', 'SampleG7', 'Value7T', 'InfoT789', 'DescG7', 'Extra7G', 'AddVal7', '2023-07-01', '2014-12-02 19:28:30'),
		(8, 'DataEig08', 'SampleH8', 'Value8S', 'InfoS890', 'DescH8', 'Extra8H', 'AddVal8', '2023-08-01', '2015-12-02 19:28:30'),
		(9, 'DataNin09', 'SampleI9', 'Value9R', 'InfoR901', 'DescI9', 'Extra9I', 'AddVal9', '2024-09-01', '2016-12-02 19:28:30'),
		(10, 'DataTen10', 'SampleJ0', 'Value0Q', 'InfoQ012', 'DescJ0', 'Extra0J', 'AddVal0', '2025-10-01', '2017-12-02 19:28:30');"""

		// prepare user
		String user = 'ranger_test_masking_user_1'
		String pwd = 'C123_567p'
		sql """DROP USER IF EXISTS ${user}"""
		sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
		// create policy
		RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
		String policy1 = 'ranger_test_masking_policy_access'
		List<String> tablePolicy = ["SELECT"]

		Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource("*"))
		resource.put("database", new RangerPolicy.RangerPolicyResource("*"))
		resource.put("table", new RangerPolicy.RangerPolicyResource([internalTable, catalogTable], false, false))
		resource.put("column", new RangerPolicy.RangerPolicyResource("*"))

		RangerPolicy policy = new RangerPolicy()
		policy.setService(rangerServiceName)
		policy.setName(policy1)
		policy.setResources(resource)

		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem()
		policyItem.setUsers([user])
		List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>()
		tablePolicy.forEach {
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


		Map<String, String> columnMaskMap = new HashMap<>()
		columnMaskMap.put("c1", "MASK")
		columnMaskMap.put("c2", "MASK_SHOW_LAST_4")
		columnMaskMap.put("c3", "MASK_SHOW_FIRST_4")
		columnMaskMap.put("c4", "MASK_HASH")
		columnMaskMap.put("c5", "MASK_NULL")
		columnMaskMap.put("c7", "CUSTOM")
		columnMaskMap.put("c6", "MASK_NONE")
		columnMaskMap.put("c8", "MASK_DATE_SHOW_YEAR")
		columnMaskMap.put("c9", "MASK_DATE_SHOW_YEAR")

		policy.setPolicyItems([])
		policy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK)
		for (Map.Entry<String, String> entry : columnMaskMap.entrySet()) {
			String column = entry.getKey()
			String maskType = entry.getValue()
			String maskPolicy = "ranger_test_masking_policy_${column}"
			policy.setName(maskPolicy)

			try {
				rangerClient.deletePolicy(rangerServiceName, maskPolicy)
			} catch (Exception e) {
				log.info("Policy not found: ${e.getMessage()}")
			}
			resource.put("column", new RangerPolicy.RangerPolicyResource(column))
			policy.setResources(resource)
			RangerPolicy.RangerDataMaskPolicyItem dataMaskPolicyItem = new RangerPolicy.RangerDataMaskPolicyItem()
			dataMaskPolicyItem.setUsers([user])
			dataMaskPolicyItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("SELECT")])
			dataMaskPolicyItem.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo(maskType, "", ""))
			if (maskType == "CUSTOM") {
				dataMaskPolicyItem.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo(maskType, "", "\"ranger test\""))
			}
			policy.setDataMaskPolicyItems([dataMaskPolicyItem])
			createdPolicy = rangerClient.createPolicy(policy)
			println("New Policy created with id: " + createdPolicy.getId())
		}
		waitPolicyEffect()
		// check
		def tokens = context.config.jdbcUrl.split('/')
		def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"
		connect("$user", "$pwd", "$defaultJdbcUrl") {
			order_qt_internal("""SELECT * FROM internal.${internalDb}.${internalTable}""")
		}
		connect("$user", "$pwd", "$defaultJdbcUrl") {
			order_qt_catalog("""SELECT * FROM ${catalog1}.${catalogDb}.${catalogTable}""")
		}
	}
}