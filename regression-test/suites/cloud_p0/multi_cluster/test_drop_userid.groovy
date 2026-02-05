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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_drop_userid", "cloud_auth") {
    def user1 = "selectdb_cloud_test_test_drop_userid"
    sql """drop user if exists ${user1}"""
    try {
        // create user
        sql """create user ${user1} IDENTIFIED BY 'A12345678_';"""
        // copy into upload, ms record userid in instancePb
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -XPUT -u ${user1}:A12345678_""")
        strBuilder.append(""" -H fileName:1.dat""")
        strBuilder.append(""" -d '1,1,2,"yyy"' """)
        def feHttpAddress = context.config.isDorisEnv ? context.config.feHttpAddress : context.config.feCloudHttpAddress
        strBuilder.append(""" -L http://""" + feHttpAddress + """/copy/upload""")
        String command = strBuilder.toString()
        logger.info("upload command=" + command)
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        Thread.sleep(2000)
        // get instance from ms can't get userid
        def instance_id = context.config.multiClusterInstance
        def ms_token = context.config.metaServiceToken
        def get_instance = { requestBody, checkFunc ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/get_instance?token=${ms_token}&instance_id=${instance_id}"
                body requestBody
                check checkFunc
            }
        }
        def jsonOutput = new JsonOutput()
        def requestBody = jsonOutput.toJson([])
        def instance_info = null
        get_instance.call(requestBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                instance_info = parseJson(body)
                assertTrue(instance_info.code.equalsIgnoreCase("OK"))
        }
        def stages = instance_info.result.stages
        if(stages != null) {
            boolean found = false
            for (def stage : stages) {
                log.info("stage info ${stage}".toString())
                // external stage doesn't contain mysql_user_name
                if ("EXTERNAL".equals(stage.type)) {
                    log.info("external stage skip".toString())
                    continue;
                }
                String name = stage.mysql_user_name[0]
                if(name.equals("${user1}".toString())) {
                    found = true
                    def userid = stage.mysql_user_id
                    assertTrue(userid != null, "before drop can find ${user1} stage in ms")
                    break
                }
            }
            assertTrue(found, "before drop can find ${user1} stage in ms")
        }

        // drop user
        sql """drop user ${user1}"""
        
        Thread.sleep(2000)
        // get instance from ms can't get userid
        jsonOutput = new JsonOutput()
        requestBody = jsonOutput.toJson([])
        instance_info = null
        get_instance.call(requestBody) {
            respCode, body ->
                log.info("http cli result: ${body} ${respCode}".toString())
                instance_info = parseJson(body)
                assertTrue(instance_info.code.equalsIgnoreCase("OK"))
        }
        stages = instance_info.result.stages
        if(stages != null) {
            boolean found = false
            for (stage : stages) {
                log.info("stage info ${stage}".toString())
                // external stage doesn't contain mysql_user_name
                if ("EXTERNAL".equals(stage.type)) {
                    log.info("external stage skip".toString())
                    continue;
                }
                String name = stage.mysql_user_name
                if(name.equals("${user1}".toString())) {
                    found = true
                    break
                }
            }
            assertFalse(found, "after drop cant find ${user1} stage in ms")
        }
    } finally {
        sql """drop user if exists ${user1}"""
    }
}

