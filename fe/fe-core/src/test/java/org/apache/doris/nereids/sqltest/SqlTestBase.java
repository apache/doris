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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.LogicalCompatibilityContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import java.util.BitSet;

public abstract class SqlTestBase extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        // make table id is larger than Integer.MAX_VALUE
        IdGeneratorBuffer idGeneratorBuffer =
                Env.getCurrentEnv().getIdGeneratorBuffer(Integer.MAX_VALUE + 10L);
        idGeneratorBuffer.getNextId();

        createTables(
                "CREATE TABLE IF NOT EXISTS T0 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T4 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "AUTO PARTITION BY LIST(`id`)\n"
                        + "(\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    protected LogicalCompatibilityContext constructContext(Plan p1, Plan p2, CascadesContext context) {
        StructInfo st1 = MaterializedViewUtils.extractStructInfo(p1, p1,
                context, new BitSet()).get(0);
        StructInfo st2 = MaterializedViewUtils.extractStructInfo(p2, p2,
                context, new BitSet()).get(0);
        RelationMapping rm = RelationMapping.generate(st1.getRelations(), st2.getRelations()).get(0);
        SlotMapping sm = SlotMapping.generate(rm);
        return LogicalCompatibilityContext.from(rm, sm, st1, st2);
    }
}
