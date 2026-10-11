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

// Policies written against a Ranger GROUP and never against the user: how a deployment runs Ranger so that
// nobody edits a policy each time somebody joins a team. Doris has no groups of its own, so these apply
// only because the ranger-doris source reads the user's groups out of Ranger's user store - the store the
// plugin downloads next to its policies. Every other suite here writes its policy items against a user.
suite("test_ranger_group_policy", "p2,ranger,external") {
	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")

	if (enabled == null || !enabled.equalsIgnoreCase("true")) {
		return
	}

	String db = 'ranger_group_db_1'
	String table = 'ranger_group_tbl_1'
	// The same database, no policy of any kind: what the group's SELECT must not open.
	String otherTable = 'ranger_group_tbl_2'
	String user = 'ranger_group_user_1'
	String pwd = 'C123_567p'
	String group = 'ranger_group_readers_1'
	String accessPolicy = 'ranger_test_group_access_policy'
	String rowFilterPolicy = 'ranger_test_group_row_filter_policy'
	String maskPolicy = 'ranger_test_group_mask_policy'
	String denyPolicy = 'ranger_test_group_deny_policy'
	// A table the user is allowed on by name while the group is denied on it; the deny has to win.
	String deniedTable = 'ranger_group_tbl_3'

	sql """CREATE DATABASE IF NOT EXISTS ${db}"""
	[table, otherTable, deniedTable].each {
		sql """DROP TABLE IF EXISTS ${db}.${it}"""
		sql """CREATE TABLE ${db}.${it} (
			id BIGINT,
			c1 VARCHAR(20),
			c2 VARCHAR(20)
		)
		DISTRIBUTED BY HASH(id) BUCKETS 2
		PROPERTIES (
			"replication_num" = "1"
		)"""
		sql """INSERT INTO ${db}.${it} VALUES
		(1, 'DataOne01', 'SampleA1'),
		(2, 'DataTwo02', 'SampleB2'),
		(3, 'DataThr03', 'SampleC3'),
		(4, 'DataFou04', 'SampleD4'),
		(5, 'DataFiv05', 'SampleE5'),
		(6, 'DataSix06', 'SampleF6'),
		(7, 'DataSev07', 'SampleG7'),
		(8, 'DataEig08', 'SampleH8'),
		(9, 'DataNin09', 'SampleI9'),
		(10, 'DataTen10', 'SampleJ0')"""
	}
	sql """DROP USER IF EXISTS ${user}"""
	sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

	// Ranger's side of the same names: the group, the user, and the membership. Nothing syncs users between
	// Doris and Ranger, so the names are simply kept equal, as the other suites do for users.
	createRangerGroup(group)
	createRangerUser(user, pwd, ["ROLE_USER"] as String[])
	setRangerUserGroups(user, [group])

	RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
	def dropPolicy = { String name ->
		try {
			rangerClient.deletePolicy(rangerServiceName, name)
		} catch (Exception e) {
			log.info("Policy not found: ${e.getMessage()}")
		}
	}
	[accessPolicy, rowFilterPolicy, maskPolicy, denyPolicy].each { dropPolicy(it) }

	def resourcesOf = { String tbl, String column ->
		Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>()
		resources.put("catalog", new RangerPolicy.RangerPolicyResource("internal"))
		resources.put("database", new RangerPolicy.RangerPolicyResource(db))
		resources.put("table", new RangerPolicy.RangerPolicyResource(tbl))
		if (column != null) {
			resources.put("column", new RangerPolicy.RangerPolicyResource(column))
		}
		return resources
	}
	def select = [new RangerPolicy.RangerPolicyItemAccess("SELECT")]

	// Four policies, four Ranger Admin calls, and the FE polls for policies every few seconds: a poll
	// between two of the calls brings a generation with only the first of them. So the access policy - the
	// one the wait below watches for - is written last: neither a row filter nor a mask nor a deny grants
	// anything, and once the FE answers on the access policy it holds the generation the other three are in.

	// 1. A row filter for the group.
	RangerPolicy policy = new RangerPolicy()
	policy.setService(rangerServiceName)
	policy.setName(rowFilterPolicy)
	policy.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER)
	policy.setResources(resourcesOf(table, null))
	RangerPolicy.RangerRowFilterPolicyItem rowFilterItem = new RangerPolicy.RangerRowFilterPolicyItem()
	rowFilterItem.setGroups([group])
	rowFilterItem.setAccesses(select)
	rowFilterItem.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id >= 5"))
	policy.setRowFilterPolicyItems([rowFilterItem])
	rangerClient.createPolicy(policy)

	// 2. A column mask for the group.
	policy = new RangerPolicy()
	policy.setService(rangerServiceName)
	policy.setName(maskPolicy)
	policy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK)
	policy.setResources(resourcesOf(table, "c1"))
	RangerPolicy.RangerDataMaskPolicyItem maskItem = new RangerPolicy.RangerDataMaskPolicyItem()
	maskItem.setGroups([group])
	maskItem.setAccesses(select)
	maskItem.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK_SHOW_LAST_4", null, null))
	policy.setDataMaskPolicyItems([maskItem])
	rangerClient.createPolicy(policy)

	// 3. A deny written against the group, on a table the user is allowed on by name. Before groups were
	// attached this deny was silently ignored and the user read the table - the dangerous half of the bug.
	policy = new RangerPolicy()
	policy.setService(rangerServiceName)
	policy.setName(denyPolicy)
	policy.setResources(resourcesOf(deniedTable, null))
	RangerPolicy.RangerPolicyItem allowUserItem = new RangerPolicy.RangerPolicyItem()
	allowUserItem.setUsers([user])
	allowUserItem.setAccesses(select)
	policy.setPolicyItems([allowUserItem])
	RangerPolicy.RangerPolicyItem denyGroupItem = new RangerPolicy.RangerPolicyItem()
	denyGroupItem.setGroups([group])
	denyGroupItem.setAccesses(select)
	policy.setDenyPolicyItems([denyGroupItem])
	rangerClient.createPolicy(policy)

	// 4. Access: SELECT on the table for the group. Last, see above.
	policy = new RangerPolicy()
	policy.setService(rangerServiceName)
	policy.setName(accessPolicy)
	policy.setResources(resourcesOf(table, null))
	RangerPolicy.RangerPolicyItem accessItem = new RangerPolicy.RangerPolicyItem()
	accessItem.setGroups([group])
	accessItem.setAccesses(select)
	policy.setPolicyItems([accessItem])
	rangerClient.createPolicy(policy)

	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"
	def readable = { String tbl ->
		return connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
			try {
				sql """SELECT * FROM internal.${db}.${tbl}"""
				return true
			} catch (Exception e) {
				log.info("not readable yet: ${e.getMessage()}")
				return false
			}
		}
	}

	// The policies reach the FE within its policy poll interval; the membership reaches it with the next
	// user store download, which the plugin makes every 60 seconds unless userStoreRefresherPollingInterval
	// in ranger-doris-security.xml says otherwise. Hence waiting on the effect rather than a fixed sleep -
	// and on the policy written last, which proves the whole set is there (see above).
	logger.info("waiting for the group's SELECT to reach ${user}")
	awaitUntil(180, 3) { readable(table) }

	connect("${user}", "${pwd}", "${defaultJdbcUrl}") {
		// Visible on the strength of the group's policy alone.
		def databases = sql """SHOW DATABASES"""
		assertTrue(databases.any { it[0] == db }, "the database the group may read is not listed")
		sql """SWITCH internal"""
		sql """USE ${db}"""

		// Rows 5..10 only, c1 masked down to its last four characters: the row filter and the mask written
		// against the group both apply, as do those written against a user.
		order_qt_group_select """SELECT * FROM internal.${db}.${table}"""

		// The group holds SELECT on one table, so the group grants nothing on the next one.
		test {
			sql """SELECT * FROM internal.${db}.${otherTable}"""
			exception "denied"
		}

		// The deny written against the group outranks the allow written against the user.
		test {
			sql """SELECT * FROM internal.${db}.${deniedTable}"""
			exception "denied"
		}
	}

	// Taking the user out of the group takes the access with it, once the plugin has downloaded the change.
	// Membership is Ranger's alone to keep, which is the whole reason a deployment grants by group.
	setRangerUserGroups(user, [])
	logger.info("waiting for ${user} to lose the group's SELECT")
	awaitUntil(180, 3) { !readable(table) }

	// And the deny written against the group no longer applies either: the user's own allow reads the table.
	awaitUntil(180, 3) { readable(deniedTable) }

	[accessPolicy, rowFilterPolicy, maskPolicy, denyPolicy].each { dropPolicy(it) }
}
