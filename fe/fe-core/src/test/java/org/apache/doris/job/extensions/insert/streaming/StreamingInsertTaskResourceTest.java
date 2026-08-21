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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

class StreamingInsertTaskResourceTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void closeReleasesExactAttemptStatementContextAndWorkerContext() throws Exception {
        StreamingInsertTask task = new StreamingInsertTask(
                1L, 2L, "", null, "", null, Collections.emptyMap(), null, null);
        ConnectContext taskContext = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        taskContext.setStatementContext(statementContext);
        taskContext.setThreadLocalInfo();
        Field contextField = StreamingInsertTask.class.getDeclaredField("ctx");
        contextField.setAccessible(true);
        contextField.set(task, taskContext);

        task.closeOrReleaseResources();
        task.closeOrReleaseResources();

        Mockito.verify(statementContext).close();
        Assertions.assertNull(task.getCtx());
        Assertions.assertNull(ConnectContext.get());
    }
}
