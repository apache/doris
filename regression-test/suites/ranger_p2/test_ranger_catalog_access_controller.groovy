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
	String hivePartialMaskName = 'ranger_ctl_hive_partial_mask'
	String dorisPolicyName = 'ranger_ctl_doris_policy'

	RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)

	def dropPolicyQuietly = { String service, String name ->
		try {
			rangerClient.deletePolicy(service, name)
		} catch (Exception e) {
			logger.info("policy ${service}/${name} not found: ${e.getMessage()}")
		}
	}

	// Waits for a policy change to become observable, rather than for a fixed duration.
	//
	// The shared waitPolicyEffect() sleeps 6 seconds, which is sized for the *doris* plugin's 5 second
	// poll interval - every sibling suite writes into the doris service only. This is the first suite to
	// write into a *hive* Ranger service, and the hive plugin polls on whatever
	// ranger.plugin.hive.policy.pollIntervalMs says in the FE's ranger-hive-security.xml, defaulting to
	// Ranger's own 30 seconds. Sleeping 6 seconds there is a race, and pinning the interval would make a
	// suite depend on a value nobody documents, so wait for the answer to change instead.
	def awaitPolicyEffect = { String what, Closure<Boolean> effective ->
		logger.info("waiting for ${what}")
		awaitUntil(180, 3) { effective() }
	}

	// Can `user` read the table through this catalog at all.
	def readable = { String catalog ->
		return connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			try {
				sql """SELECT * FROM ${catalog}.${dbName}.${tblName}"""
				return true
			} catch (Exception e) {
				return false
			}
		}
	}

	// How many rows `user` sees through this catalog, which is what a row filter changes.
	def visibleRows = { String catalog ->
		return connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			return sql("""SELECT id FROM ${catalog}.${dbName}.${tblName}""").size()
		}
	}

	// Whether every username `user` sees through this catalog is masked away.
	def usernamesMasked = { String catalog ->
		return connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			def rows = sql("""SELECT username FROM ${catalog}.${dbName}.${tblName}""")
			return !rows.isEmpty() && rows.every { it[0] == null }
		}
	}

	// Grants `user` `accesses` on `resources` of `service`. `effective` says how to tell it has been
	// picked up; without one the doris plugin's poll interval is waited out.
	def grant = { String service, String name, Map<String, RangerPolicy.RangerPolicyResource> resources,
			List<String> accesses, Closure<Boolean> effective = null ->
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
		if (effective == null) {
			waitPolicyEffect()
		} else {
			awaitPolicyEffect("policy ${name} in ${service} to take effect", effective)
		}
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

	// Everything this suite owns is dropped here, before it is used, rather than in a finally block:
	// what a failed run leaves behind is what makes it debuggable (root AGENTS.md, test convention 3).
	dropPolicyQuietly(HIVE_SERVICE_NAME, hivePolicyName)
	dropPolicyQuietly(HIVE_SERVICE_NAME, hiveRowFilterName)
	dropPolicyQuietly(HIVE_SERVICE_NAME, hiveMaskName)
	dropPolicyQuietly(HIVE_SERVICE_NAME, hivePartialMaskName)
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
	grant(HIVE_SERVICE_NAME, hivePolicyName, hiveResources, ["select"], { readable(boundCatalog) })
	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		order_qt_opened_by_the_hive_policy """SELECT * FROM ${boundCatalog}.${dbName}.${tblName}"""
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
	awaitPolicyEffect("the hive policy to stop opening the table", { !readable(boundCatalog) })
	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		test {
			sql """SELECT * FROM ${boundCatalog}.${dbName}.${tblName}"""
			exception "does not have privilege"
		}
	}

	// case 5: the same source can be named by the class of the factory publishing it, which is
	// what releases before the sources became plugins wrote, and what a catalog created then
	// still has persisted.
	grant(HIVE_SERVICE_NAME, hivePolicyName, hiveResources, ["select"], { readable(boundCatalog) })
	sql """DROP CATALOG IF EXISTS ${fqcnCatalog}"""
	sql """CREATE CATALOG ${fqcnCatalog} PROPERTIES (
		"type"="hms",
		'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
		'access_controller.class' = '${FACTORY_CLASS}',
		'access_controller.properties.ranger.service.name' = '${HIVE_SERVICE_NAME}'
	)"""
	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		order_qt_selected_by_the_factory_class_name """SELECT * FROM ${fqcnCatalog}.${dbName}.${tblName}"""
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
	awaitPolicyEffect("the row filter to reach the bound catalog", { visibleRows(boundCatalog) == 2 })

	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		// The same table, read through the two authorities: filtered by the one whose service the filter
		// was written in, whole through the one that has no source of its own.
		order_qt_row_filter_through_the_bound_catalog """SELECT id FROM ${boundCatalog}.${dbName}.${tblName}"""
		order_qt_row_filter_through_the_plain_catalog """SELECT id FROM ${plainCatalog}.${dbName}.${tblName}"""
	}
	dropPolicyQuietly(HIVE_SERVICE_NAME, hiveRowFilterName)
	awaitPolicyEffect("the row filter to be gone again", { visibleRows(boundCatalog) == 4 })

	// case 7: and the same for a column mask. MASK_NULL first, because its effect needs no agreement about
	// string shapes: the column comes back null through the bound catalog and intact through the plain one.
	// Case 8 covers the mask types whose payload the service definition supplies.
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
	awaitPolicyEffect("the column mask to reach the bound catalog", { usernamesMasked(boundCatalog) })

	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		order_qt_mask_through_the_bound_catalog """SELECT username FROM ${boundCatalog}.${dbName}.${tblName}"""
		order_qt_mask_through_the_plain_catalog """SELECT username FROM ${plainCatalog}.${dbName}.${tblName}"""
	}

	// case 8: a mask type whose payload the stock Hive service definition writes as a Hive UDF.
	//
	// This is the branch nothing else covers. MASK_NULL above short circuits three branches before the one
	// that consults the service definition, and the definition's own transformer for this mask type is
	// mask_show_last_n(...), a Hive UDF Doris has no function for - so without the translation in
	// RangerHiveAccessController this statement fails on an unknown function, with nothing in the error
	// pointing at Ranger. Before the access type fix in this series it failed differently and worse: the
	// lookup matched no policy at all and the column came back in the clear.
	dropPolicyQuietly(HIVE_SERVICE_NAME, hiveMaskName)
	awaitPolicyEffect("the nullifying mask to be gone again", { !usernamesMasked(boundCatalog) })

	RangerPolicy partialMask = new RangerPolicy()
	partialMask.setService(HIVE_SERVICE_NAME)
	partialMask.setName(hivePartialMaskName)
	partialMask.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK)
	partialMask.setResources([
			'database': new RangerPolicy.RangerPolicyResource(dbName),
			'table'   : new RangerPolicy.RangerPolicyResource(tblName),
			'column'  : new RangerPolicy.RangerPolicyResource('username')
	])
	RangerPolicy.RangerDataMaskPolicyItem partialMaskItem = new RangerPolicy.RangerDataMaskPolicyItem()
	partialMaskItem.setUsers([user])
	partialMaskItem.setAccesses([new RangerPolicy.RangerPolicyItemAccess("select")])
	partialMaskItem.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK_SHOW_LAST_4", "", ""))
	partialMask.setDataMaskPolicyItems([partialMaskItem])
	dropPolicyQuietly(HIVE_SERVICE_NAME, hivePartialMaskName)
	logger.info("created partial data mask policy id ${rangerClient.createPolicy(partialMask).getId()}")

	// LPAD(RIGHT(col, 4), CHAR_LENGTH(col), 'X') - what ranger-servicedef-doris.json declares for this mask
	// type and therefore what the hive source translates it to. Computed here rather than recorded in an .out
	// file, so that a wrong translation reads as a wrong value rather than as a baseline somebody refreshed.
	def showLast4Of = { String value ->
		String tail = value.length() <= 4 ? value : value.substring(value.length() - 4)
		return 'X' * (value.length() - tail.length()) + tail
	}
	awaitPolicyEffect("the partial column mask to reach the bound catalog", {
		connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			try {
				return sql("""SELECT username FROM ${boundCatalog}.${dbName}.${tblName} WHERE id = 1""")[0][0] !=
						'alice'
			} catch (Exception e) {
				// A mask Doris cannot bind fails the statement, which is the very failure this case exists to
				// catch - reported by the assertion below rather than waited out into a timeout here.
				logger.info("the partial mask is not applied yet, or cannot be applied: ${e.getMessage()}")
				return false
			}
		}
	})

	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		def masked = sql("""SELECT username FROM ${boundCatalog}.${dbName}.${tblName} ORDER BY id""")
				.collect { it[0] }
		assertEquals(['alice', 'bob', 'carol', 'dave'].collect { showLast4Of(it) }, masked,
				"a MASK_SHOW_LAST_4 policy on a hive service did not reach the query as the Doris rendering"
						+ " of that mask type")
		// The negative control the other data policy cases have: read through the catalog with no source of
		// its own, the column is untouched.
		assertEquals(['alice', 'bob', 'carol', 'dave'],
				sql("""SELECT username FROM ${plainCatalog}.${dbName}.${tblName} ORDER BY id""").collect { it[0] })
	}
}
