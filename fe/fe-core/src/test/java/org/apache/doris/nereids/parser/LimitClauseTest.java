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

package org.apache.doris.nereids.parser;

import org.junit.jupiter.api.Test;

public class LimitClauseTest extends ParserTestBase {
    @Test
    public void testLimit() {
        parsePlan("SELECT b FROM test order by a limit 3 offset 100")
                .matches(
                        logicalLimit(
                                logicalSort()
                        ).when(limit -> limit.getLimit() == 3 && limit.getOffset() == 100)
                );

        parsePlan("SELECT b FROM test order by a limit 100, 3")
                .matches(
                        logicalLimit(
                                logicalSort()
                        ).when(limit -> limit.getLimit() == 3 && limit.getOffset() == 100)
                );

        parsePlan("SELECT b FROM test limit 3")
                .matches(logicalLimit().when(limit -> limit.getLimit() == 3 && limit.getOffset() == 0));

        parsePlan("SELECT b FROM test order by a limit 3")
                .matches(
                        logicalLimit(
                                logicalSort()
                        ).when(limit -> limit.getLimit() == 3 && limit.getOffset() == 0)
                );
    }

    @Test
    public void testNoLimit() {
        parsePlan("select a from tbl order by x").matches(logicalSort());
    }

    @Test
    public void testNoQueryOrganization() {
        parsePlan("select a from tbl")
                .matches(
                        logicalProject(
                                logicalCheckPolicy(
                                        unboundRelation()
                                )
                        )
                );
    }
}
