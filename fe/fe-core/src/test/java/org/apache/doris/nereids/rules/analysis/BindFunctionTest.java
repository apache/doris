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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class BindFunctionTest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "CREATE TABLE t1 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t2 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");"
        );
    }

    @Test
    public void testTimeArithmExpr() {
        // TODO: need to fix the UT for datev2
        if (!Config.enable_date_conversion) {
            String sql = "SELECT * FROM t1 WHERE col1 < date '1994-01-01' + interval '1' year";

            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalFilter(logicalOlapScan())
                                    .when(f -> ((LessThan) f.getPredicate()).right() instanceof DateLiteral)
                    );
        }
    }

    @Test
    void testJoinBindFunction() {
        String sql = "SELECT * FROM t1 LEFT JOIN t2 ON abs(t1.col2) = t2.col2 where t1.col2 > 10";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        leftOuterLogicalJoin(
                                logicalProject(logicalFilter()),
                                logicalProject(logicalOlapScan())
                        ).when(join -> join.getHashJoinConjuncts().size() == 1)
                );
    }
}
