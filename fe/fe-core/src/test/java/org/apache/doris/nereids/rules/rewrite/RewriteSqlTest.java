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

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

class RewriteSqlTest extends SqlTestBase {

    @Test
    void testExtractSingleTableExpressionFromDisjunction() throws Exception {
        createTables(
                "CREATE TABLE IF NOT EXISTS test_extract_single_1 (\n"
                        + "    a int,\n"
                        + "    b int,\n"
                        + "    c int,\n"
                        + "    d int\n"
                        + ")\n"
                        + "DUPLICATE KEY(a)\n"
                        + "DISTRIBUTED BY HASH(a) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        String sql = "select * from test_extract_single_1 t1, T2 where t1.a = 1 or t1.b = 2 and (t1.c = 3 or t1.d = 4 and T2.id = 5)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matches(logicalFilter().when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("OR[(a = 1),AND[(b = 2),OR[(c = 3),(d = 4)]]]"))));
    }

}
