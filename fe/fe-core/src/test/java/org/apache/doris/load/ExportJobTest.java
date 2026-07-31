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

package org.apache.doris.load;

import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;

class ExportJobTest {

    @Test
    void testOutfilePlansUseExportJobStatementStartTime() {
        Instant statementStartTime = Instant.parse("2026-06-16T05:06:07.123456Z");
        ExportJob exportJob = new ExportJob(1L);
        exportJob.setStatementStartTime(statementStartTime);
        exportJob.setStatementTimeZone("+08:00");

        LogicalPlanAdapter firstAdapter = (LogicalPlanAdapter) exportJob.generateLogicalPlanAdapter(
                Mockito.mock(LogicalPlan.class));
        LogicalPlanAdapter secondAdapter = (LogicalPlanAdapter) exportJob.generateLogicalPlanAdapter(
                Mockito.mock(LogicalPlan.class));

        Assertions.assertEquals(statementStartTime,
                firstAdapter.getStatementContext().getStatementStartTime());
        Assertions.assertEquals(statementStartTime,
                secondAdapter.getStatementContext().getStatementStartTime());
        Assertions.assertEquals("+08:00", firstAdapter.getStatementContext().getStatementTimeZone().getId());
        Assertions.assertEquals("+08:00", secondAdapter.getStatementContext().getStatementTimeZone().getId());
    }
}
