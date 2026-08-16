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

/**
 * An authorization source bound to one catalog rather than to the whole instance.
 *
 * The sibling suites all exercise the instance wide source named by fe.conf's access_controller_type.
 * This one covers the other way a Ranger source is selected: a catalog naming it in
 * access_controller.class. That splits the decision in two, which is what the cases below pin:
 * catalog level questions stay with the instance wide source, everything inside the catalog goes to
 * the source the catalog is bound to. Reaching a table therefore needs a grant in each of the two
 * Ranger services, and taking either one away is enough to close it again.
 *
 * The last two cases carry that split into row filters and column masks, which are asked of whoever
 * answers for the table: the same table read through a bound catalog and through one without a source
 * of its own comes back filtered and masked in the first, whole and in the clear in the second.
 *
 * Prerequisites beyond the ones the sibling suites need:
 *
 *   - ranger-hive-security.xml in the FE's conf directory. RangerBasePlugin is handed the catalog's
 *     access_controller.properties.ranger.service.name as the service *type*, so that property picks
 *     the file (ranger-<type>-security.xml) while the file's ranger.plugin.hive.service.name picks
 *     the Ranger service instance. HIVE_SERVICE_NAME below has to match that property.
 *   - The ranger-hive plugin installed under plugins/authorization, which the release ships.
 *
 * The Ranger service instance is created here: it is this suite's fixture, not the environment's.
 */
suite("test_ranger_catalog_access_controller", "p2,ranger,external") {
	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String dorisServiceName = context.config.otherConfigs.get("rangerServiceName")
	String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
	String hmsPort = context.config.otherConfigs.get("hive3HmsPort")

	if (enabled == null || !enabled.equalsIgnoreCase("true")) {
		logger.info("skip test_ranger_catalog_access_controller because enableRangerTest is not true")
		return
	}

	// Must equal ranger.plugin.hive.service.name in the FE's ranger-hive-security.xml.
	String HIVE_SERVICE_NAME = 'hive'
	String FACTORY_CLASS = 'org.apache.doris.catalog.authorizer.ranger.hive.RangerHiveAccessControllerFactory'

	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"

	String boundCatalog = 'ranger_ctl_bound'
	String fqcnCatalog = 'ranger_ctl_fqcn'
	String plainCatalog = 'ranger_ctl_plain'
	String dbName = 'ranger_ctl_db'
	String tblName = 'ranger_ctl_tbl'
	String user = 'ranger_ctl_user'
	String pwd = 'C123_567p'
	String hivePolicyName = 'ranger_ctl_hive_policy'
	String hiveRowFilterName = 'ranger_ctl_hive_row_filter'
	String hiveMaskName = 'ranger_ctl_hive_mask'
	String dorisPolicyName = 'ranger_ctl_doris_policy'

	RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)

	def dropPolicyQuietly = { String service, String name ->
		try {
			rangerClient.deletePolicy(service, name)
		} catch (Exception e) {
			logger.info("policy ${service}/${name} not found: ${e.getMessage()}")
		}
	}

	// Grants `user` `accesses` on `resources` of `service`, and waits for the plugin to pick it up.
	def grant = { String service, String name, Map<String, RangerPolicy.RangerPolicyResource> resources,
			List<String> accesses ->
		dropPolicyQuietly(service, name)
		RangerPolicy policy = new RangerPolicy()
		policy.setService(service)
		policy.setName(name)
		policy.setResources(resources)
		RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem()
		item.setUsers([user])
		item.setAccesses(accesses.collect { new RangerPolicy.RangerPolicyItemAccess(it) })
		policy.setPolicyItems([item])
		logger.info("created policy ${name} with id ${rangerClient.createPolicy(policy).getId()}")
		waitPolicyEffect()
	}

	// ---- the Ranger service the bound catalog answers to ----
	// Its default policies grant the configured user everything, which is what lets root build the
	// fixture below through a catalog this service governs.
	try {
		rangerClient.getService(HIVE_SERVICE_NAME)
		logger.info("ranger service ${HIVE_SERVICE_NAME} already exists")
	} catch (Exception e) {
		logger.info("creating ranger service ${HIVE_SERVICE_NAME}: ${e.getMessage()}")
		RangerService service = new RangerService()
		service.setName(HIVE_SERVICE_NAME)
		service.setType('hive')
		service.setDescription('Doris catalog level authorization regression service')
		// jdbc.* are read only by the admin UI's resource lookup, but the service definition requires
		// them, so a service cannot be created without values here.
		service.setConfigs([
				'username'            : 'root',
				'password'            : 'root',
				'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver',
				'jdbc.url'            : "jdbc:hive2://${externalEnvIp}:10000".toString()
		])
		rangerClient.createService(service)
	}

	// ---- fixture ----
	// Built through a catalog with no source of its own, so root's grants in the instance wide
	// service cover all of it. Both catalogs are the same metastore, so it is the same table.
	sql """DROP CATALOG IF EXISTS ${plainCatalog}"""
	sql """CREATE CATALOG ${plainCatalog} PROPERTIES (
		"type"="hms",
		'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
	)"""
	sql """DROP DATABASE IF EXISTS ${plainCatalog}.${dbName} FORCE"""
	sql """CREATE DATABASE ${plainCatalog}.${dbName}"""
	sql """CREATE TABLE ${plainCatalog}.${dbName}.${tblName} (
		id BIGINT,
		username VARCHAR(20)
	) ENGINE=hive PROPERTIES ('file_format'='parquet')"""
	sql """INSERT INTO ${plainCatalog}.${dbName}.${tblName} VALUES
		(1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')"""

	sql """DROP CATALOG IF EXISTS ${boundCatalog}"""
	sql """CREATE CATALOG ${boundCatalog} PROPERTIES (
		"type"="hms",
		'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
		'access_controller.class' = 'ranger-hive',
		'access_controller.properties.ranger.service.name' = '${HIVE_SERVICE_NAME}'
	)"""

	sql """DROP USER IF EXISTS ${user}"""
	sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

	dropPolicyQuietly(HIVE_SERVICE_NAME, hivePolicyName)
	dropPolicyQuietly(dorisServiceName, dorisPolicyName)

	Map<String, RangerPolicy.RangerPolicyResource> hiveResources = [
			'database': new RangerPolicy.RangerPolicyResource(dbName),
			'table'   : new RangerPolicy.RangerPolicyResource(tblName),
			'column'  : new RangerPolicy.RangerPolicyResource('*')
	]
	// The plain catalog is in here so that the last two cases can read the same table through both
	// authorities. It has no source of its own, so this grant covers all of it; the two bound ones
	// route everything below the catalog elsewhere, which is what case 4 turns on.
	Map<String, RangerPolicy.RangerPolicyResource> dorisResources = [
			'catalog': new RangerPolicy.RangerPolicyResource([boundCatalog, fqcnCatalog, plainCatalog],
					false, false)
	]
	List<String> dorisAccesses = ["SELECT", "LOAD", "ALTER", "CREATE", "DROP", "SHOW_VIEW"]

	try {
		// case 1: what is inside the catalog is the bound source's to answer, and it has been given
		// nothing. The refusal names the table, in the shape a Hive service phrases it: no catalog,
		// because a Hive service has no such scope.
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			test {
				sql """SELECT * FROM ${boundCatalog}.${dbName}.${tblName}"""
				exception "does not have privilege"
			}
		}

		// case 2: a policy in that service, and only in that service, opens the table.
		// Hive access types are lower case; RangerHiveAccessController maps a Doris SELECT onto this.
		grant(HIVE_SERVICE_NAME, hivePolicyName, hiveResources, ["select"])
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			def rows = sql """SELECT * FROM ${boundCatalog}.${dbName}.${tblName}"""
			assertEquals(4, rows.size())
		}

		// case 3: the catalog itself is a different question, and the bound source never sees it.
		// Reading a qualified table above asked about the table alone; SWITCH asks about the catalog,
		// which routes to the instance wide source, where nothing has been granted yet.
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			test {
				sql """SWITCH ${boundCatalog}"""
				exception "to catalog"
			}
		}
		grant(dorisServiceName, dorisPolicyName, dorisResources, dorisAccesses)
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			sql """SWITCH ${boundCatalog}"""
		}

		// case 4: and that catalog grant reaches no further than the catalog. It is still in force
		// here, so the table closing again when the Hive policy goes away is the bound source's
		// doing: the two services each answer their own half and neither covers for the other.
		dropPolicyQuietly(HIVE_SERVICE_NAME, hivePolicyName)
		waitPolicyEffect()
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			test {
				sql """SELECT * FROM ${boundCatalog}.${dbName}.${tblName}"""
				exception "does not have privilege"
			}
		}

		// case 5: the same source can be named by the class of the factory publishing it, which is
		// what releases before the sources became plugins wrote, and what a catalog created then
		// still has persisted.
		grant(HIVE_SERVICE_NAME, hivePolicyName, hiveResources, ["select"])
		sql """DROP CATALOG IF EXISTS ${fqcnCatalog}"""
		sql """CREATE CATALOG ${fqcnCatalog} PROPERTIES (
			"type"="hms",
			'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
			'access_controller.class' = '${FACTORY_CLASS}',
			'access_controller.properties.ranger.service.name' = '${HIVE_SERVICE_NAME}'
		)"""
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			def rows = sql """SELECT * FROM ${fqcnCatalog}.${dbName}.${tblName}"""
			assertEquals(4, rows.size())
		}

		// case 6: a row filter is decided by the same source as the read it applies to, so writing
		// one in the bound catalog's service changes what that catalog returns and leaves the plain
		// catalog, whose authority is the instance wide service, returning the whole table. Same
		// metastore, same table, two answers.
		RangerPolicy rowFilter = new RangerPolicy()
		rowFilter.setService(HIVE_SERVICE_NAME)
		rowFilter.setName(hiveRowFilterName)
		rowFilter.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER)
		// A Hive row filter is written against the table; a column resource is not part of that def.
		rowFilter.setResources([
				'database': new RangerPolicy.RangerPolicyResource(dbName),
				'table'   : new RangerPolicy.RangerPolicyResource(tblName)
		])
		RangerPolicy.RangerRowFilterPolicyItem rowFilterItem = new RangerPolicy.RangerRowFilterPolicyItem()
		rowFilterItem.setUsers([user])
		rowFilterItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
		rowFilterItem.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id >= 3"))
		rowFilter.setRowFilterPolicyItems([rowFilterItem])
		dropPolicyQuietly(HIVE_SERVICE_NAME, hiveRowFilterName)
		logger.info("created row filter policy id ${rangerClient.createPolicy(rowFilter).getId()}")
		waitPolicyEffect()

		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			def filtered = sql """SELECT id FROM ${boundCatalog}.${dbName}.${tblName}"""
			assertEquals(2, filtered.size())
			filtered.each { assertTrue(it[0] >= 3, "row ${it[0]} should have been filtered out") }

			def whole = sql """SELECT id FROM ${plainCatalog}.${dbName}.${tblName}"""
			assertEquals(4, whole.size())
		}
		dropPolicyQuietly(HIVE_SERVICE_NAME, hiveRowFilterName)
		waitPolicyEffect()

		// case 7: and the same for a column mask. MASK_NULL is the one whose effect needs no
		// agreement about string shapes: the column comes back null through the bound catalog and
		// intact through the plain one.
		RangerPolicy mask = new RangerPolicy()
		mask.setService(HIVE_SERVICE_NAME)
		mask.setName(hiveMaskName)
		mask.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK)
		// A mask is written against one column, which is why Ranger evaluates masks a column at a time.
		mask.setResources([
				'database': new RangerPolicy.RangerPolicyResource(dbName),
				'table'   : new RangerPolicy.RangerPolicyResource(tblName),
				'column'  : new RangerPolicy.RangerPolicyResource('username')
		])
		RangerPolicy.RangerDataMaskPolicyItem maskItem = new RangerPolicy.RangerDataMaskPolicyItem()
		maskItem.setUsers([user])
		maskItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
		maskItem.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK_NULL", "", ""))
		mask.setDataMaskPolicyItems([maskItem])
		dropPolicyQuietly(HIVE_SERVICE_NAME, hiveMaskName)
		logger.info("created data mask policy id ${rangerClient.createPolicy(mask).getId()}")
		waitPolicyEffect()

		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			def masked = sql """SELECT username FROM ${boundCatalog}.${dbName}.${tblName}"""
			assertEquals(4, masked.size())
			masked.each { assertTrue(it[0] == null, "username should have been masked, got ${it[0]}") }

			def clear = sql """SELECT username FROM ${plainCatalog}.${dbName}.${tblName}"""
			assertEquals(4, clear.size())
			clear.each { assertTrue(it[0] != null, "username should not be masked here") }
		}
	} finally {
		dropPolicyQuietly(HIVE_SERVICE_NAME, hivePolicyName)
		dropPolicyQuietly(HIVE_SERVICE_NAME, hiveRowFilterName)
		dropPolicyQuietly(HIVE_SERVICE_NAME, hiveMaskName)
		dropPolicyQuietly(dorisServiceName, dorisPolicyName)
		sql """DROP CATALOG IF EXISTS ${boundCatalog}"""
		sql """DROP CATALOG IF EXISTS ${fqcnCatalog}"""
		sql """DROP DATABASE IF EXISTS ${plainCatalog}.${dbName} FORCE"""
		sql """DROP CATALOG IF EXISTS ${plainCatalog}"""
		sql """DROP USER IF EXISTS ${user}"""
	}
}
