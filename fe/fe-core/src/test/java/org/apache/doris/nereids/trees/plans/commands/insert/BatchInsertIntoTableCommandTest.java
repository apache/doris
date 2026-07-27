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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class BatchInsertIntoTableCommandTest {
    @Test
    void testWaitForAutoStartBeforeTakingTableLock() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        UnboundTableSink<InlineTable> logicalPlan = Mockito.mock(UnboundTableSink.class);
        InlineTable inlineTable = Mockito.mock(InlineTable.class);
        Mockito.when(logicalPlan.child()).thenReturn(inlineTable);
        BatchInsertIntoTableCommand command = new BatchInsertIntoTableCommand(logicalPlan);

        ConnectContext context = new ConnectContext();
        context.setCloudCluster("current-compute-group");
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        OlapTable targetTable = Mockito.mock(OlapTable.class);
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        DdlException autoStartFailure = new DdlException("compute group is manually shut down");
        Mockito.when(systemInfoService.waitForAutoStart("current-compute-group")).thenThrow(autoStartFailure);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<InsertUtils> insertUtilsStatic = Mockito.mockStatic(InsertUtils.class)) {
            Config.cloud_unique_id = "test-cloud";
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            insertUtilsStatic.when(() -> InsertUtils.getTargetTable(logicalPlan, context)).thenReturn(targetTable);

            DdlException thrown = Assertions.assertThrows(
                    DdlException.class, () -> command.run(context, stmtExecutor));

            Assertions.assertSame(autoStartFailure, thrown);
            Mockito.verify(systemInfoService).waitForAutoStart("current-compute-group");
            Mockito.verify(targetTable, Mockito.never()).readLock();
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
