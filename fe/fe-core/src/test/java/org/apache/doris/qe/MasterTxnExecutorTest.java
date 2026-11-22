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

package org.apache.doris.qe;

import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

class MasterTxnExecutorTest {
    @ParameterizedTest
    @CsvSource({"10, 10000", "2147483, 2147483000", "2147484, 2147483647", "2147483647, 2147483647"})
    void testTimeoutConversion(int seconds, int expectedMillis) {
        // The constructor used to turn 2147484 seconds into a negative RPC timeout.
        // One context is enough to inspect both stored timeouts. No transaction or RPC is needed.
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getExecTimeoutS()).thenReturn(seconds);
        MasterTxnExecutor executor = new MasterTxnExecutor(context);
        int waitTimeout = Deencapsulation.getField(executor, "waitTimeoutMs");
        int thriftTimeout = Deencapsulation.getField(executor, "thriftTimeoutMs");
        Assertions.assertEquals(expectedMillis, waitTimeout);
        Assertions.assertEquals(expectedMillis, thriftTimeout);
    }
}
