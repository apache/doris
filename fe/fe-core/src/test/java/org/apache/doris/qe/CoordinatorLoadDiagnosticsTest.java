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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class CoordinatorLoadDiagnosticsTest {
    private static final int FRAGMENT_ID = 7;
    private static final long BACKEND_ID = 9;
    private static final String TRACKING_URL = "http://127.0.0.1/error-log";
    private static final String FIRST_ERROR_MSG = "invalid integer. Src line: invalid";

    @Test
    void publishesDiagnosticsBeforeFailureReleasesWaiters() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        Coordinator coordinator = new Coordinator(-1L, new TUniqueId(12345, 6), new DescriptorTable(),
                Collections.singletonList(fragment()), Collections.emptyList(), "UTC", false, false) {
            @Override
            protected void cancelInternal(Status cancelReason) {
                super.cancelInternal(cancelReason);
                // Inspect the result at the exact point where a load task can return from join(),
                // before the report handler resumes. No scheduling delay is needed to expose the race.
                Assertions.assertTrue(join(1));
                Assertions.assertEquals(TStatusCode.DATA_QUALITY_ERROR, getExecStatus().getErrorCode());
                Assertions.assertEquals(TRACKING_URL, getTrackingUrl());
                Assertions.assertEquals(FIRST_ERROR_MSG, getFirstErrorMsg());
                cancelled.set(true);
            }
        };
        prepareReport(coordinator);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report(TStatusCode.DATA_QUALITY_ERROR)));

        Assertions.assertTrue(cancelled.get());
    }

    @Test
    void retainsDiagnosticsOnNormalCompletion() throws Exception {
        Coordinator coordinator = new Coordinator(-1L, new TUniqueId(12345, 7), new DescriptorTable(),
                Collections.singletonList(fragment()), Collections.emptyList(), "UTC", false, false);
        prepareReport(coordinator);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report(TStatusCode.OK)));

        Assertions.assertTrue(coordinator.join(1));
        Assertions.assertTrue(coordinator.getExecStatus().ok());
        Assertions.assertEquals(TRACKING_URL, coordinator.getTrackingUrl());
        Assertions.assertEquals(FIRST_ERROR_MSG, coordinator.getFirstErrorMsg());
    }

    private static PlanFragment fragment() {
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(FRAGMENT_ID));
        return fragment;
    }

    private static TReportExecStatusParams report(TStatusCode statusCode) {
        return new TReportExecStatusParams()
                .setFragmentId(FRAGMENT_ID)
                .setBackendId(BACKEND_ID)
                .setDone(true)
                .setStatus(new TStatus(statusCode))
                .setTrackingUrl(TRACKING_URL)
                .setFirstErrorMsg(FIRST_ERROR_MSG);
    }

    @SuppressWarnings("unchecked")
    private static void prepareReport(Coordinator coordinator) throws Exception {
        Coordinator.PipelineExecContext context = Mockito.mock(Coordinator.PipelineExecContext.class);
        Mockito.when(context.updatePipelineStatus(Mockito.any())).thenReturn(true);
        Field contextsField = Coordinator.class.getDeclaredField("pipelineExecContexts");
        contextsField.setAccessible(true);
        Map<Pair<Integer, Long>, Coordinator.PipelineExecContext> contexts =
                (Map<Pair<Integer, Long>, Coordinator.PipelineExecContext>) contextsField.get(coordinator);
        contexts.put(Pair.of(FRAGMENT_ID, BACKEND_ID), context);

        MarkedCountDownLatch<Integer, Long> latch = new MarkedCountDownLatch<>(1);
        latch.addMark(FRAGMENT_ID, BACKEND_ID);
        Field latchField = Coordinator.class.getDeclaredField("fragmentsDoneLatch");
        latchField.setAccessible(true);
        latchField.set(coordinator, latch);
    }
}
