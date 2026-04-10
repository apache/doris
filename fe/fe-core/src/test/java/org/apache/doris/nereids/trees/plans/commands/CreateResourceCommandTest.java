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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CreateResourceCommandTest extends TestWithFeService {
    @Test
    public void testValidate() {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessControllerManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        // ES resource is no longer supported, validation should throw
        final ImmutableMap<String, String> esProperties =
                ImmutableMap.of("type", "es", "host", "http://127.0.0.1:29200");
        final CreateResourceInfo esInfo = new CreateResourceInfo(true, false, "test", esProperties);
        final CreateResourceCommand esCommand = new CreateResourceCommand(esInfo);
        // validate() itself doesn't reject ES type, but resource creation does.
        // The validate step just checks type parsing, which still succeeds for ES.
        Assertions.assertDoesNotThrow(() -> esCommand.getInfo().validate());

        // jfs/juicefs should be treated as HDFS-compatible resource type
        final ImmutableMap<String, String> jfsProperties =
                ImmutableMap.of("type", "jfs", "fs.defaultFS", "jfs://cluster");
        final CreateResourceInfo jfsInfo = new CreateResourceInfo(true, false, "test_jfs", jfsProperties);
        final CreateResourceCommand jfsCommand = new CreateResourceCommand(jfsInfo);
        Assertions.assertDoesNotThrow(() -> jfsCommand.getInfo().validate());

        // test validate abnormal
        // test properties
        final CreateResourceInfo nullPropertiesInfo = new CreateResourceInfo(false, false, "test", null);
        final CreateResourceCommand createResourceCommand1 = new CreateResourceCommand(nullPropertiesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> createResourceCommand1.getInfo().validate());

        // test resource type
        final ImmutableMap<String, String> noTypeProperties = ImmutableMap.of("host", "http://127.0.0.1:29200");
        final CreateResourceInfo noTypeInfo = new CreateResourceInfo(false, false, "test", noTypeProperties);
        final CreateResourceCommand createResourceCommand2 = new CreateResourceCommand(noTypeInfo);
        Assertions.assertThrows(AnalysisException.class, () -> createResourceCommand2.getInfo().validate());

        // test unsupported resource type
        final ImmutableMap<String, String> unsupportedTypeProperties =
                ImmutableMap.of("type", "flink", "host", "http://127.0.0.1:29200");
        final CreateResourceInfo unsupportedTypeInfo =
                new CreateResourceInfo(false, false, "test", unsupportedTypeProperties);
        final CreateResourceCommand createResourceCommand3 = new CreateResourceCommand(unsupportedTypeInfo);
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
                + " \"driver_url\" = \"https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/jdbc_driver/mysql-connector-java-8.0.25.jar\",\n"
                + " \"driver_class\" = \"com.mysql.cj.jdbc.Driver\"\n"
                + ");";

        // ES resource creation should fail since ES resources are no longer supported
        Assertions.assertThrows(DdlException.class, () -> createResource(es));
        Assertions.assertDoesNotThrow(() -> createResource(jdbc));
    }

    private void createResource(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateResourceCommand);
        ((CreateResourceCommand) plan).run(connectContext, null);
    }
}
