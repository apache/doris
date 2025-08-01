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

Suite.metaClass.waitPolicyEffect {
	sleep(6000)
	// TODO: check if policy is effective by API
}