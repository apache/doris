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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class PushDownTopNDistinctThroughUnionTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createTable("CREATE TABLE push_down_topn_distinct_union_t (\n"
                + "  zzc int NULL,\n"
                + "  sy_zcjlr int NULL\n"
                + ") ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(zzc) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    void testUnionDistinctWithDuplicateOutputAndWindow() {
        String sql = "SELECT *\n"
                + "FROM (\n"
                + "  SELECT *\n"
                + "  FROM (\n"
                + "    SELECT zzc, sy_zcjlr, sy_zcjlr, ROW_NUMBER() OVER (ORDER BY zzc DESC)\n"
                + "    FROM push_down_topn_distinct_union_t\n"
                + "  ) u1\n"
                + "  UNION\n"
                + "  SELECT *\n"
                + "  FROM (\n"
                + "    SELECT zzc, sy_zcjlr, sy_zcjlr, ROW_NUMBER() OVER (ORDER BY zzc DESC)\n"
                + "    FROM push_down_topn_distinct_union_t\n"
                + "  ) u2\n"
                + ") u\n"
                + "ORDER BY 1\n"
                + "LIMIT 10";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalUnion(
                                logicalTopN().when(topN -> topN.getLimit() == 10 && topN.getOffset() == 0),
                                logicalTopN().when(topN -> topN.getLimit() == 10 && topN.getOffset() == 0)
                        )
                );
    }
}
