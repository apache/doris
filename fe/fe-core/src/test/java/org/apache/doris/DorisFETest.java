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

package org.apache.doris;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

public class DorisFETest {

    // A frontend that can not write its edit log exits through the shutdown hook. Waiting there for running
    // queries (up to 300 seconds) would keep it serving metadata that can no longer be persisted, while every
    // thread that writes the edit log stays blocked with its locks held.
    @Test
    public void testGracefulShutdownDoesNotWaitAfterEditLogWriteFailure() throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        TUniqueId queryId = new TUniqueId(10L, 20L);
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(coordinator));
        Deencapsulation.setField(EditLog.class, "writeFailed", true);
        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), DorisFE::gracefulShutdown);
        } finally {
            Deencapsulation.setField(EditLog.class, "writeFailed", false);
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }
}
