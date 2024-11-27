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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class SetOperationTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `t1` (\n"
                + " `k1` bigint(20) NULL,\n"
                + " `k2` bigint(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + " `k1` bigint(20) NULL,\n"
                + " `k2` varchar(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t3` (\n"
                + " `k1` varchar(20) NULL,\n"
                + " `k2` varchar(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    // union
    @Test
    public void testUnion1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1, k2 from t1 union select k1, k2 from t3;");
    }

    @Test
    public void testUnion2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1, k2 from t1 union all select k1, k2 from t3;");
    }

    @Test
    public void testUnion3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select k1 from t1 union select k1 from t3 union select 1;");
    }

    @Test
    public void testUnion4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select 1 a, 2 b union all select 3, 4 union all select 10 e, 20 f;");
    }

    @Test
    public void testUnion5() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("select 1, 2 union all select 1, 2 union all select 10 e, 20 f;");
    }

    @Test
    public void testUnion6() {
        LogicalOneRowRelation first = new LogicalOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(
                        new Alias(new Concat(new StringLiteral("1"), new StringLiteral("1")))
        ));

        UnboundOneRowRelation second = new UnboundOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(
                    new UnboundAlias(new UnboundFunction(
                            "concat",
                            ImmutableList.of(new StringLiteral("2"), new StringLiteral("2")))
                    )
        ));

        LogicalOneRowRelation third = new LogicalOneRowRelation(
                RelationId.createGenerator().getNextId(), ImmutableList.of(
                    new Alias(new Concat(new StringLiteral("3"), new StringLiteral("3")))
        ));

        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(
                first, second, third
        ));
        PlanChecker.from(connectContext, union)
                .analyze()
                .rewrite();
    }
}
