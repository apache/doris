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
    void testExtractSingleTableExpressionFromDisjunction() {
        String sql = "select * from T1, T2 where T1.id = 1 or T1.score = 2 and (T1.a = 3 or T1.b = 4 and T2.id = 5)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matches(logicalFilter().when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("OR[(id = 1),AND[(score = 2),OR[(a = 3),(b = 4)]]]"))));
    }

}
