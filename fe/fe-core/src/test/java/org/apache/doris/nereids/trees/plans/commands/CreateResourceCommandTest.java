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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateResourceCommandTest extends TestWithFeService {
    @Test
    public void testValidate(@Mocked Env env, @Mocked AccessControllerManager accessManager) {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // test validate normal
        ImmutableMap<String, String> properties = ImmutableMap.of("type", "es", "host", "http://127.0.0.1:29200");
        CreateResourceInfo info = new CreateResourceInfo(true, false, "test", properties);
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(info);
        Assertions.assertDoesNotThrow(() -> createResourceCommand.getInfo().validate());

        // test validate abnormal
        // test properties
        info = new CreateResourceInfo(false, false, "test", null);
        CreateResourceCommand createResourceCommand1 = new CreateResourceCommand(info);
        Assertions.assertThrows(AnalysisException.class, () -> createResourceCommand1.getInfo().validate());

        // test resource type
        properties = ImmutableMap.of("host", "http://127.0.0.1:29200");
        info = new CreateResourceInfo(false, false, "test", properties);
        CreateResourceCommand createResourceCommand2 = new CreateResourceCommand(info);
        Assertions.assertThrows(AnalysisException.class, () -> createResourceCommand2.getInfo().validate());

        // test unsupported resource type
        properties = ImmutableMap.of("type", "flink", "host", "http://127.0.0.1:29200");
        info = new CreateResourceInfo(false, false, "test", properties);
        CreateResourceCommand createResourceCommand3 = new CreateResourceCommand(info);
        Assertions.assertThrows(AnalysisException.class, () -> createResourceCommand3.getInfo().validate());
    }

    @Test
    public void testCreateResource() {
        String es = "CREATE RESOURCE \"es_resource\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + " \"type\"=\"es\",\n"
                + " \"hosts\"=\"http://127.0.0.1:29200\",\n"
                + " \"nodes_discovery\"=\"false\",\n"
                + " \"enable_keyword_sniff\"=\"true\"\n"
                + ");";

        String jdbc = "CREATE EXTERNAL RESOURCE \"jdbc_resource\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + " \"type\" = \"jdbc\",\n"
                + " \"user\" = \"jdbc_user\",\n"
                + " \"password\" = \"jdbc_passwd\",\n"
                + " \"jdbc_url\" = \"jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false\",\n"
                + " \"driver_url\" = \"https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar\",\n"
                + " \"driver_class\" = \"com.mysql.cj.jdbc.Driver\"\n"
                + ");";

        Assertions.assertDoesNotThrow(() -> createResource(es));
        Assertions.assertDoesNotThrow(() -> createResource(jdbc));
    }

    private void createResource(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateResourceCommand);
        ((CreateResourceCommand) plan).run(connectContext, null);
    }
}
