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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.Suite

Suite.metaClass.createRangerUser = { String user, String password, String[] roles ->
	def jsonOutput = new JsonOutput()
	def rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	def rangerUser = context.config.otherConfigs.get("rangerUser")
	def rangerPassword = context.config.otherConfigs.get("rangerPassword")
	def map = [
		"name":"${user}",
		"firstName":"${user}",
		"lastName": "${user}",
		"loginId": "${user}",
		"emailAddress" : null,
		"description" : "${user} desc",
		"password" : "${password}",
		"status":1,
		"isVisible":1,
		"userRoleList": roles,
		"userSource": 0]
	def js = jsonOutput.toJson(map)
	log.info("create user req: ${js} ".toString())
	def createUserApi = { request_body, check_func ->
		httpTest {
			basicAuthorization "${rangerUser}","${rangerPassword}"
			endpoint "${rangerEndpoint}"
			uri "/service/xusers/secure/users"
			body request_body
			op "post"
			check check_func
		}
	}

	createUserApi.call(js) {
		respCode, body ->
			log.info("create user resp: ${body} ${respCode}".toString())
			assertTrue(respCode == 200 || body.contains("Error creating duplicate object"))
	}
}

Suite.metaClass.dropRangerUser = { String userId ->
	def rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	def rangerUser = context.config.otherConfigs.get("rangerUser")
	def rangerPassword = context.config.otherConfigs.get("rangerPassword")
	def map = []
	def jsonOutput = new JsonOutput()
	def js = jsonOutput.toJson(map)
	def dropUserApi = { check_func ->
		httpTest {
			basicAuthorization "${rangerUser}","${rangerPassword}"
			endpoint "${rangerEndpoint}"
			uri "/service/xusers/secure/users/${userId}"
			op "delete"
			body js
			check check_func
		}
	}

	dropUserApi.call {
		respCode, body ->
			log.info("drop user resp: ${body} ${respCode}".toString())
			assertTrue(respCode == 204)
	}
}

// A group in Ranger's own user store: what a policy item is written against so that nobody edits the policy
// each time somebody joins a team. Doris has no groups, so the ranger-doris source reads a user's groups out of
// this store; the suites put a user into a group here and write policies against the group only.
// Tolerates a group that is already there, the way createRangerUser tolerates an existing user.
Suite.metaClass.createRangerGroup = { String group ->
	def rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	def rangerUser = context.config.otherConfigs.get("rangerUser")
	def rangerPassword = context.config.otherConfigs.get("rangerPassword")
	def js = new JsonOutput().toJson([
		"name": "${group}",
		"description": "${group} desc",
		"groupType": 1,
		"groupSource": 0,
		"isVisible": 1])
	log.info("create group req: ${js} ".toString())
	httpTest {
		basicAuthorization "${rangerUser}","${rangerPassword}"
		endpoint "${rangerEndpoint}"
		uri "/service/xusers/secure/groups"
		body js
		op "post"
		check { respCode, body ->
			log.info("create group resp: ${body} ${respCode}".toString())
			assertTrue(respCode == 200 || body.contains("Error creating duplicate object"))
		}
	}
}

// Reads one object out of Ranger Admin's user/group API as a map.
Suite.metaClass.getRangerXObject = { String path ->
	def rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	def rangerUser = context.config.otherConfigs.get("rangerUser")
	def rangerPassword = context.config.otherConfigs.get("rangerPassword")
	def object = null
	httpTest {
		basicAuthorization "${rangerUser}","${rangerPassword}"
		endpoint "${rangerEndpoint}"
		header "Accept", "application/json"
		uri path
		op "get"
		check { respCode, body ->
			assertEquals(200, respCode, "GET ${path}: ${body}")
			object = new JsonSlurper().parseText(body)
		}
	}
	return object
}

// Rewrites the groups a Ranger user is in, to exactly `groups`. Goes through the user update that the
// Ranger UI uses rather than the group-user mapping API, because in Ranger Admin 2.4 only the former bumps
// the user store version - and the version is what tells the plugins there is a new store to download; a
// membership changed without it is invisible to every plugin until something else bumps it.
Suite.metaClass.setRangerUserGroups = { String user, List<String> groups ->
	def rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	def rangerUser = context.config.otherConfigs.get("rangerUser")
	def rangerPassword = context.config.otherConfigs.get("rangerPassword")
	def vxUser = getRangerXObject("/service/xusers/users/userName/${user}")
	vxUser.groupIdList = groups.collect { getRangerXObject("/service/xusers/groups/groupName/${it}").id }
	vxUser.groupNameList = groups
	def js = new JsonOutput().toJson(vxUser)
	log.info("set user groups req: ${js} ".toString())
	httpTest {
		basicAuthorization "${rangerUser}","${rangerPassword}"
		endpoint "${rangerEndpoint}"
		uri "/service/xusers/secure/users/${vxUser.id}"
		body js
		op "put"
		check { respCode, body ->
			log.info("set user groups resp: ${body} ${respCode}".toString())
			assertEquals(200, respCode)
		}
	}
}

Suite.metaClass.waitPolicyEffect {
	sleep(6000)
	// TODO: check if policy is effective by API
}
