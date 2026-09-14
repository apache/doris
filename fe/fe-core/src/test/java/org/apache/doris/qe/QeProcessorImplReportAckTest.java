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
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.cache.Cache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class QeProcessorImplReportAckTest {
    private TUniqueId registeredQueryId;

    @AfterEach
    void cleanup() {
        if (registeredQueryId != null) {
            QeProcessorImpl.INSTANCE.unregisterQuery(registeredQueryId);
        }
    }

    @Test
    void rejectsExternalReportWithoutCoordinator() {
        TReportExecStatusResult result = report(params(new TUniqueId(12345, 1)));

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isExternalFileCommitDataAccepted());
    }

    @Test
    void rejectsExternalReportWhenHandlerThrows() throws Exception {
        TUniqueId queryId = new TUniqueId(12345, 2);
        Coordinator coordinator = register(queryId);
        Mockito.doThrow(new RuntimeException("injected acceptance failure"))
                .when(coordinator).updateFragmentExecStatus(Mockito.any());

        TReportExecStatusResult result = report(params(queryId));

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isExternalFileCommitDataAccepted());
    }

    @Test
    void rejectsExternalReportWhenHandlerDoesNotAcceptIt() throws Exception {
        TUniqueId queryId = new TUniqueId(12345, 4);
        register(queryId);

        TReportExecStatusResult result = report(params(queryId));

        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertFalse(result.isExternalFileCommitDataAccepted());
    }

    @Test
    void retriesAcceptedExternalReportAfterCoordinatorRemoval() throws Exception {
        TUniqueId queryId = new TUniqueId(12345, 3);
        Coordinator coordinator = register(queryId);
        Mockito.when(coordinator.updateFragmentExecStatus(Mockito.any())).thenReturn(true);
        TReportExecStatusParams params = params(queryId);

        TReportExecStatusResult first = report(params);
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        registeredQueryId = null;
        TReportExecStatusResult retry = report(params);

        Assertions.assertTrue(first.isExternalFileCommitDataAccepted());
        Assertions.assertTrue(retry.isExternalFileCommitDataAccepted());
        Assertions.assertEquals(TStatusCode.OK, retry.getStatus().getStatusCode());
        Mockito.verify(coordinator, Mockito.times(1)).updateFragmentExecStatus(params);
    }

    @Test
    void evictedAcceptanceTokenRejectsRetryAfterCoordinatorRemoval() throws Exception {
        TUniqueId queryId = new TUniqueId(12345, 5);
        Coordinator coordinator = register(queryId);
        Mockito.when(coordinator.updateFragmentExecStatus(Mockito.any())).thenReturn(true);
        TReportExecStatusParams params = params(queryId);

        TReportExecStatusResult first = report(params);
        QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        registeredQueryId = null;
        acceptedExternalFileReports().invalidateAll();
        TReportExecStatusResult retry = report(params);

        Assertions.assertTrue(first.isExternalFileCommitDataAccepted());
        Assertions.assertFalse(retry.isExternalFileCommitDataAccepted());
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, retry.getStatus().getStatusCode());
        Mockito.verify(coordinator, Mockito.times(1)).updateFragmentExecStatus(params);
    }

    @Test
    void coordinatorReportStateUsesCrossThreadVisibility() throws Exception {
        Field done = Coordinator.PipelineExecContext.class.getDeclaredField("done");
        Field txnId = Coordinator.class.getDeclaredField("txnId");

        Assertions.assertTrue(Modifier.isVolatile(done.getModifiers()));
        Assertions.assertTrue(Modifier.isVolatile(txnId.getModifiers()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void legacyCoordinatorPublishesTrackingUrlBeforeCancellation() throws Exception {
        int fragmentId = 7;
        long backendId = 9;
        String trackingUrl = "http://127.0.0.1/error-log";
        AtomicReference<String> trackingUrlAtCancel = new AtomicReference<>();

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(fragmentId));
        Coordinator coordinator = new Coordinator(-1L, new TUniqueId(12345, 6), new DescriptorTable(),
                Collections.singletonList(fragment), Collections.emptyList(), "UTC", false, false) {
            @Override
            protected void cancelInternal(Status cancelReason) {
                trackingUrlAtCancel.set(getTrackingUrl());
            }
        };

        TReportExecStatusParams report = new TReportExecStatusParams()
                .setFragmentId(fragmentId)
                .setBackendId(backendId)
                .setDone(true)
                .setStatus(new TStatus(TStatusCode.DATA_QUALITY_ERROR))
                .setTrackingUrl(trackingUrl);
        Coordinator.PipelineExecContext context = Mockito.mock(Coordinator.PipelineExecContext.class);
        Mockito.when(context.updatePipelineStatus(report)).thenReturn(true);

        Field contextsField = Coordinator.class.getDeclaredField("pipelineExecContexts");
        contextsField.setAccessible(true);
        Map<Pair<Integer, Long>, Coordinator.PipelineExecContext> contexts =
                (Map<Pair<Integer, Long>, Coordinator.PipelineExecContext>) contextsField.get(coordinator);
        contexts.put(Pair.of(fragmentId, backendId), context);

        MarkedCountDownLatch<Integer, Long> fragmentsDoneLatch = new MarkedCountDownLatch<>(1);
        fragmentsDoneLatch.addMark(fragmentId, backendId);
        Field latchField = Coordinator.class.getDeclaredField("fragmentsDoneLatch");
        latchField.setAccessible(true);
        latchField.set(coordinator, fragmentsDoneLatch);

        coordinator.updateFragmentExecStatus(report);

        Assertions.assertEquals(trackingUrl, trackingUrlAtCancel.get());
    }

    @Test
    void legacyCoordinatorRetriesFailedAcceptanceBeforeMarkingDone() {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        ExecutionProfile profile = Mockito.mock(ExecutionProfile.class);
        Coordinator.PipelineExecContext context = new Coordinator.PipelineExecContext(
                new PlanFragmentId(7), null, backend, profile, -1);
        TReportExecStatusParams report = new TReportExecStatusParams().setDone(true);

        Assertions.assertTrue(context.updatePipelineStatus(report));
        context.finishPipelineStatus(false);
        Assertions.assertTrue(context.updatePipelineStatus(report));
        context.finishPipelineStatus(true);
        Assertions.assertFalse(context.updatePipelineStatus(report));
    }

    private Coordinator register(TUniqueId queryId) throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(coordinator));
        registeredQueryId = queryId;
        return coordinator;
    }

    private static TReportExecStatusParams params(TUniqueId queryId) {
        return new TReportExecStatusParams()
                .setQueryId(queryId)
                .setFragmentId(7)
                .setBackendId(9)
                .setDone(true)
                .setStatus(new TStatus(TStatusCode.OK))
                .setIcebergCommitDatas(Collections.<TIcebergCommitData>emptyList());
    }

    private static TReportExecStatusResult report(TReportExecStatusParams params) {
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, new TNetworkAddress("127.0.0.1", 9050));
    }

    @SuppressWarnings("unchecked")
    private static Cache<String, Boolean> acceptedExternalFileReports() throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("acceptedExternalFileReports");
        field.setAccessible(true);
        return (Cache<String, Boolean>) field.get(QeProcessorImpl.INSTANCE);
    }
}
