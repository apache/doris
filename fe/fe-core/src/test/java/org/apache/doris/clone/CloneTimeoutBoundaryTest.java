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

package org.apache.doris.clone;

import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CloneTimeoutBoundaryTest {
    @Test
    void schedulerDeadlineFitsCloneRequest() {
        long savedMin = Config.min_clone_task_timeout_sec;
        long savedMax = Config.max_clone_task_timeout_sec;
        try {
            Config.min_clone_task_timeout_sec = 2147483648L;
            Config.max_clone_task_timeout_sec = 2147483648L;
            TabletSchedCtx ctx = Mockito.mock(TabletSchedCtx.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(0L).when(ctx).getTabletSize();
            // No tablet data is required. Both configured bounds exceed TCloneReq's i32 seconds.
            long timeout = Deencapsulation.invoke(ctx, "getApproximateTimeoutMs");
            Assertions.assertEquals(2147483647000L, timeout);
        } finally {
            Config.min_clone_task_timeout_sec = savedMin;
            Config.max_clone_task_timeout_sec = savedMax;
        }
    }
}
