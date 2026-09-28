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
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class CoordinatorLoadDiagnosticsTest {
    private static final int FRAGMENT_ID = 7;
    private static final long BACKEND_ID = 9;
    private static final String TRACKING_URL = "http://127.0.0.1/error-log";
    private static final String FIRST_ERROR_MSG = "invalid integer. Src line: invalid";
    private static final Map<String, String> COUNTERS = Map.of(
            LoadEtlTask.DPP_NORMAL_ALL, "10", LoadEtlTask.DPP_ABNORMAL_ALL, "1", LoadJob.UNSELECTED_ROWS, "3");

    @Test
    void publishesLoadResultBeforeFailureReleasesWaiters() throws Exception {
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
                assertLoadResult(this, FRAGMENT_ID);
                cancelled.set(true);
            }
        };
        prepareReports(coordinator, FRAGMENT_ID);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(
                report(FRAGMENT_ID, TStatusCode.DATA_QUALITY_ERROR)));

        Assertions.assertTrue(cancelled.get());
        assertLoadResult(coordinator, FRAGMENT_ID);
    }

    @Test
    void retainsLoadResultOnNormalCompletion() throws Exception {
        Coordinator coordinator = coordinator();
        prepareReports(coordinator, FRAGMENT_ID);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report(FRAGMENT_ID, TStatusCode.OK)));

        Assertions.assertTrue(coordinator.join(1));
        Assertions.assertTrue(coordinator.getExecStatus().ok());
        assertLoadResult(coordinator, FRAGMENT_ID);
    }

    @Test
    void ignoresDuplicateFailureReport() throws Exception {
        Coordinator coordinator = coordinator();
        prepareReports(coordinator, FRAGMENT_ID);
        TReportExecStatusParams report = report(FRAGMENT_ID, TStatusCode.DATA_QUALITY_ERROR);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report));
        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report));

        assertLoadResult(coordinator, FRAGMENT_ID);
    }

    @Test
    void preservesImmutableSnapshotsWhenAnotherFragmentReportsAfterFailure() throws Exception {
        Coordinator coordinator = coordinator();
        prepareReports(coordinator, FRAGMENT_ID, FRAGMENT_ID + 1);
        Assertions.assertTrue(coordinator.updateFragmentExecStatus(
                report(FRAGMENT_ID, TStatusCode.DATA_QUALITY_ERROR)));
        Assertions.assertTrue(coordinator.join(1));

        Map<String, String> counters = coordinator.getLoadCounters();
        List<String> deltaUrls = coordinator.getDeltaUrls();
        List<TTabletCommitInfo> commitInfos = coordinator.getCommitInfos();
        List<TErrorTabletInfo> errorTabletInfos = coordinator.getErrorTabletInfos();

        Assertions.assertThrows(UnsupportedOperationException.class, counters::clear);
        Assertions.assertThrows(UnsupportedOperationException.class, deltaUrls::clear);
        Assertions.assertThrows(UnsupportedOperationException.class, commitInfos::clear);
        Assertions.assertThrows(UnsupportedOperationException.class, errorTabletInfos::clear);
        assertLoadResult(coordinator, FRAGMENT_ID);

        Assertions.assertTrue(coordinator.updateFragmentExecStatus(report(FRAGMENT_ID + 1, TStatusCode.OK)));

        Assertions.assertEquals(COUNTERS, counters);
        Assertions.assertEquals(Collections.singletonList(deltaUrl(FRAGMENT_ID)), deltaUrls);
        Assertions.assertEquals(Collections.singletonList(commitInfo(FRAGMENT_ID)), commitInfos);
        Assertions.assertEquals(Collections.singletonList(errorTabletInfo(FRAGMENT_ID)), errorTabletInfos);
        Assertions.assertEquals(Map.of(LoadEtlTask.DPP_NORMAL_ALL, "20", LoadEtlTask.DPP_ABNORMAL_ALL, "2",
                LoadJob.UNSELECTED_ROWS, "6"), coordinator.getLoadCounters());
        Assertions.assertEquals(List.of(deltaUrl(FRAGMENT_ID), deltaUrl(FRAGMENT_ID + 1)), coordinator.getDeltaUrls());
        Assertions.assertEquals(List.of(commitInfo(FRAGMENT_ID), commitInfo(FRAGMENT_ID + 1)),
                coordinator.getCommitInfos());
        Assertions.assertEquals(List.of(errorTabletInfo(FRAGMENT_ID), errorTabletInfo(FRAGMENT_ID + 1)),
                coordinator.getErrorTabletInfos());
    }

    private static Coordinator coordinator() {
        return new Coordinator(-1L, new TUniqueId(12345, 7), new DescriptorTable(),
                Collections.singletonList(fragment()), Collections.emptyList(), "UTC", false, false);
    }

    private static PlanFragment fragment() {
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(FRAGMENT_ID));
        return fragment;
    }

    private static TReportExecStatusParams report(int fragmentId, TStatusCode statusCode) {
        return new TReportExecStatusParams()
                .setFragmentId(fragmentId)
                .setBackendId(BACKEND_ID)
                .setDone(true)
                .setStatus(new TStatus(statusCode))
                .setTrackingUrl(TRACKING_URL)
                .setFirstErrorMsg(FIRST_ERROR_MSG)
                .setLoadCounters(COUNTERS)
                .setDeltaUrls(Collections.singletonList(deltaUrl(fragmentId)))
                .setCommitInfos(Collections.singletonList(commitInfo(fragmentId)))
                .setErrorTabletInfos(Collections.singletonList(errorTabletInfo(fragmentId)));
    }

    private static String deltaUrl(int fragmentId) {
        return "http://127.0.0.1/delta/" + fragmentId;
    }

    private static TTabletCommitInfo commitInfo(int fragmentId) {
        return new TTabletCommitInfo(fragmentId, BACKEND_ID);
    }

    private static TErrorTabletInfo errorTabletInfo(int fragmentId) {
        return new TErrorTabletInfo().setTabletId(fragmentId).setMsg(FIRST_ERROR_MSG);
    }

    private static void assertLoadResult(Coordinator coordinator, int fragmentId) {
        Assertions.assertEquals(TRACKING_URL, coordinator.getTrackingUrl());
        Assertions.assertEquals(FIRST_ERROR_MSG, coordinator.getFirstErrorMsg());
        Assertions.assertEquals(COUNTERS, coordinator.getLoadCounters());
        Assertions.assertEquals(Collections.singletonList(deltaUrl(fragmentId)), coordinator.getDeltaUrls());
        Assertions.assertEquals(Collections.singletonList(commitInfo(fragmentId)), coordinator.getCommitInfos());
        Assertions.assertEquals(Collections.singletonList(errorTabletInfo(fragmentId)),
                coordinator.getErrorTabletInfos());
    }

    @SuppressWarnings("unchecked")
    private static void prepareReports(Coordinator coordinator, int... fragmentIds) throws Exception {
        Field contextsField = Coordinator.class.getDeclaredField("pipelineExecContexts");
        contextsField.setAccessible(true);
        Map<Pair<Integer, Long>, Coordinator.PipelineExecContext> contexts =
                (Map<Pair<Integer, Long>, Coordinator.PipelineExecContext>) contextsField.get(coordinator);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(BACKEND_ID);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        ExecutionProfile profile = Mockito.mock(ExecutionProfile.class);
        MarkedCountDownLatch<Integer, Long> latch = new MarkedCountDownLatch<>(fragmentIds.length);
        for (int fragmentId : fragmentIds) {
            Coordinator.PipelineExecContext context = new Coordinator.PipelineExecContext(
                    new PlanFragmentId(fragmentId), null, backend, profile, -1);
            contexts.put(Pair.of(fragmentId, BACKEND_ID), context);
            latch.addMark(fragmentId, BACKEND_ID);
        }
        setField(coordinator, "fragmentsDoneLatch", latch);
        // exec() initializes these containers before dispatching a load plan to backends.
        setField(coordinator, "deltaUrls", new ArrayList<String>());
        setField(coordinator, "loadCounters", new HashMap<String, String>());
    }

    private static void setField(Coordinator coordinator, String name, Object value) throws Exception {
        Field field = Coordinator.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(coordinator, value);
    }
}
