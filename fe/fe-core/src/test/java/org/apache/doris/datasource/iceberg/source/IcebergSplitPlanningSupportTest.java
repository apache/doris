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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class IcebergSplitPlanningSupportTest {
    @Test
    public void testRewriteTasksForceSyncPlanningMode() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        StatementContext statementContext = new StatementContext();
        statementContext.setConnectContext(ctx);
        ctx.setStatementContext(statementContext);
        FileScanTask rewriteTask = Mockito.mock(FileScanTask.class);
        statementContext.setIcebergRewriteFileScanTasks(Collections.singletonList(rewriteTask));

        try {
            IcebergSplitPlanningSupport support = new IcebergSplitPlanningSupport(
                    Mockito.mock(IcebergScanNode.class),
                    Mockito.mock(IcebergSource.class),
                    Mockito.mock(Table.class),
                    Mockito.mock(ExecutionAuthenticator.class),
                    new SessionVariable(),
                    ScanContext.EMPTY,
                    Collections.emptyMap(),
                    IcebergScanNode.MIN_DELETE_FILE_SUPPORT_VERSION,
                    false);

            Assert.assertFalse(support.isBatchMode());
            Assert.assertEquals(Collections.singletonList(rewriteTask),
                    statementContext.getAndClearIcebergRewriteFileScanTasks());
        } finally {
            ConnectContext.remove();
        }
    }
}
