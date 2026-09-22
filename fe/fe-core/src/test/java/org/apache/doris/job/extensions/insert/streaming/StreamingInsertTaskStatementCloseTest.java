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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class StreamingInsertTaskStatementCloseTest {

    /**
     * The task runs on the streaming scheduler, not under TaskProcessor, so nobody else ends the
     * statement of an attempt: releasing the attempt's resources must close its StatementContext,
     * which stops the scan nodes of the plan before() built only to rewrite the TVF and that no
     * coordinator ever took - a remote Doris scan joined with the TVF keeps a Flight SQL session on
     * the other frontend until then.
     */
    @Test
    public void testReleasingAnAttemptEndsItsStatement() {
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.getSourceType()).thenReturn("s3");
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, "insert into t select 1", provider, "test_db",
                Mockito.mock(StreamingJobProperties.class), Collections.emptyMap(), UserIdentity.ROOT, null);
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement("insert into t select 1", 0));
        ctx.setStatementContext(statementContext);
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        statementContext.stopScanNodeAtClose(scanNode);
        Deencapsulation.setField(task, "ctx", ctx);

        task.closeOrReleaseResources();

        Mockito.verify(scanNode).stop();
        Assertions.assertNull(Deencapsulation.getField(task, "ctx"));
    }
}
