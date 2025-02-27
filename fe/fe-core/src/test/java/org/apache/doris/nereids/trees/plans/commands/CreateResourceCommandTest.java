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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateResourceCommandTest extends TestWithFeService {

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

    public void createResource(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateResourceCommand);
        ((CreateResourceCommand) plan).run(connectContext, null);
    }
}
