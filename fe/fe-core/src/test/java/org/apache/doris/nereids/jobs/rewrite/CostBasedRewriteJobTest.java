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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CostBasedRewriteJobTest {

    @Test
    void testGetOriginSqlForLoggingWithoutOriginStatement() {
        StatementContext statementContext = new StatementContext(null, null);

        Assertions.assertEquals("<unknown>",
                CostBasedRewriteJob.getOriginSqlForLogging(statementContext));
    }

    @Test
    void testGetOriginSqlForLoggingWithOriginStatement() {
        StatementContext statementContext = new StatementContext(
                null, new OriginStatement("select 1", 0));

        Assertions.assertEquals("select 1",
                CostBasedRewriteJob.getOriginSqlForLogging(statementContext));
    }
}
