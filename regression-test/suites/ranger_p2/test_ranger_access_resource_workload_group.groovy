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


suite("test_ranger_access_workload_group", "p2,ranger,external") {
	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl = tokens[0] + "//" + tokens[2] + "/?"

	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")

	if (enabled != null && enabled.equalsIgnoreCase("true")) {
		// prepare workload group
		List<String> workloadGroupList = ['ranger_wg1', 'ranger_wg2']
		workloadGroupList.forEach {
			sql """DROP WORKLOAD GROUP IF EXISTS ${it}"""
			sql """CREATE WORKLOAD GROUP ${it} properties ("min_cpu_percent"="1");"""
		}
		// prepare user
		List<String> userList = ['ranger_test_wg_user1', 'ranger_test_wg_user2']
		String pwd = 'C123_567p'
		userList.forEach {
			sql """DROP USER IF EXISTS ${it}"""
			sql """CREATE USER '${it}' IDENTIFIED BY '${pwd}'"""
		}

		// case1
		// create policy
		RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
		String policy1 = 'ranger_test_workload_group_policy_1'
		List<String> workloadGroupPolicy = ["USAGE"]
		Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>()
		resource.put("workload_group", new RangerPolicy.RangerPolicyResource(workloadGroupList[0]))
		RangerPolicy policy = new RangerPolicy()
		policy.setService(rangerServiceName)
		policy.setName(policy1)
		policy.setResources(resource)
		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem()
		policyItem.setUsers([userList[0]])

		List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>()
		workloadGroupPolicy.forEach {
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
		connect("${userList[0]}", "$pwd", "$defaultJdbcUrl") {
			def ret = sql("""SHOW WORKLOAD GROUPS LIKE \"${workloadGroupList[0]}\"""")
			assertTrue(ret.size() > 0)
			ret = sql("""SHOW WORKLOAD GROUPS LIKE \"${workloadGroupList[1]}\"""")
			assertTrue(ret.size() == 0)
		}
		// case2
		String policy2 = 'all - workload_group'
		policy.setName(policy2)
		resource.clear()
		resource.put("workload_group", new RangerPolicy.RangerPolicyResource("*"))
		policy.setResources(resource)
		policyItem.setUsers([userList[1], "admin", "root"])
		rangerClient.updatePolicy(rangerServiceName, "all%20-%20workload_group", policy)
		waitPolicyEffect()
		connect("${userList[1]}", "$pwd", "$defaultJdbcUrl") {
			def ret = sql("""SHOW WORKLOAD GROUPS LIKE \"${workloadGroupList[0]}\"""")
			assertTrue(ret.size() > 0)
			ret = sql("""SHOW WORKLOAD GROUPS LIKE \"${workloadGroupList[1]}\"""")
			assertTrue(ret.size() > 0)
		}
	}
}
