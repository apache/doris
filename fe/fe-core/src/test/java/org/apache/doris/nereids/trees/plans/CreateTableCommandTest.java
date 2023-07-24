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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateTableCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    public void testCreateSimpleTable() throws Exception {
        String ddl = "create table t(" 
                + "    id int," 
                + "    v1 int" 
                + ")" 
                + "distributed by hash(id) buckets 10 " 
                + "properties(" 
                + "   \"replication_num\"=\"1\"" 
                + ")\n";
        LogicalPlan plan = new NereidsParser().parseSingle(ddl);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableCommand command = ((CreateTableCommand) plan);
        command.run(connectContext, null);
    }
    
    @Test
    public void testCreateTableWithAllField() throws Exception {
        String ddl = "create table agg_light_sc_not_null_nop_t (\n"
                + "    `id` int not null,\n"
                + "    `kbool` boolean replace not null,\n"
                + "    `ktint` tinyint(4) max not null,\n"
                + "    `ksint` smallint(6) max not null,\n"
                + "    `kint` int(11) max not null,\n"
                + "    `kbint` bigint(20) max not null,\n"
                + "    `klint` largeint(40) max not null,\n"
                + "    `kfloat` float max not null,\n"
                + "    `kdbl` double max not null,\n"
                + "    `kdcml` decimal(12, 6) replace not null,\n"
                + "    `kchr` char(10) replace not null,\n"
                + "    `kvchr` varchar(10) replace not null,\n"
                + "    `kstr` string replace not null,\n"
                + "    `kdt` date replace not null,\n"
                + "    `kdtv2` datev2 replace not null,\n"
                + "    `kdtm` datetime replace not null,\n"
                + "    `kdtmv2` datetimev2(0) replace not null,\n"
                + "    `kdcml32v3` decimalv3(7, 3) replace not null,\n"
                + "    `kdcml64v3` decimalv3(10, 5) replace not null,\n"
                + "    `kdcml128v3` decimalv3(20, 8) replace not null\n"
                + ") engine=OLAP\n"
                + "aggregate key(id)\n"
                + "distributed by hash(id) buckets 4\n"
                + "properties (\n"
                + "   \"replication_num\"=\"1\",\n"
                + "   \"light_schema_change\"=\"true\"\n"
                + ")\n";
        LogicalPlan plan = new NereidsParser().parseSingle(ddl);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableCommand command = ((CreateTableCommand) plan);
        command.run(connectContext, null);
    }
}
