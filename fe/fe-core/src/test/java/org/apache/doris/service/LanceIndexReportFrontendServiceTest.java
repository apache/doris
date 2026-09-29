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

package org.apache.doris.service;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobManager;
import org.apache.doris.datasource.lance.job.LanceIndexJobResult;
import org.apache.doris.datasource.lance.job.LanceIndexJobResultCode;
import org.apache.doris.datasource.lance.job.LanceIndexTerminationProof;
import org.apache.doris.thrift.TLanceIndexJobReport;
import org.apache.doris.thrift.TLanceIndexJobResultCode;
import org.apache.doris.thrift.TLanceIndexJobTerminationReport;
import org.apache.doris.thrift.TLanceIndexTerminationProof;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Coverage for the report entry point
 * {@link FrontendServiceImpl#reportLanceIndexJobResult(TLanceIndexJobReport)}: the layer
 * must stay thin. Only the master accepts a report (a non-master answers NOT_MASTER and
 * never touches the job manager), and on the master the envelope is handed to the report
 * handler verbatim once its invocation-secret echo authenticates against the durable
 * record: the identity quad and every typed field of the classified result must
 * reach {@link LanceIndexJobManager#completeWithResult} unchanged, and a CHILD_REAPED
 * proof must reach {@link LanceIndexJobManager#recordTerminationProof} with the durable
 * backend id as its source.
 */
public class LanceIndexReportFrontendServiceTest {
    private static final long JOB_ID = 1L;
    private static final long DISPATCH_REVISION = 1L;
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final String INVOCATION_SECRET = "a3f1c02d97b64e8fad0c31b9e75d2468";

    @Test
    public void nonMasterRejectsTheReportWithoutTouchingTheJobManager() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        LanceIndexJobManager manager = Mockito.mock(LanceIndexJobManager.class);
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStatus status = service.reportLanceIndexJobResult(matchingReport());
            Assertions.assertEquals(TStatusCode.NOT_MASTER, status.getStatusCode());
        }
        Mockito.verifyNoInteractions(manager);
    }

    @Test
    public void masterDelegatesTheIdentityQuadAndTypedResultVerbatim() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        LanceIndexJobManager manager = Mockito.mock(LanceIndexJobManager.class);
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
        // Built before the stubbing: constructing and stubbing the job mock inside
        // when(...) triggers Mockito's unfinished-stubbing detection.
        LanceIndexJob dispatched = dispatchedJob();
        Mockito.when(manager.getJob(JOB_ID)).thenReturn(dispatched);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStatus status = service.reportLanceIndexJobResult(matchingReport()
                    .setResultCode(TLanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT)
                    .setSanitizedMessage("commit conflict on the provider")
                    .setExternalMetadataAdvanced(true));
            Assertions.assertEquals(TStatusCode.OK, status.getStatusCode());
        }

        ArgumentCaptor<LanceIndexJobResult> captor = ArgumentCaptor.forClass(LanceIndexJobResult.class);
        Mockito.verify(manager).completeWithResult(Mockito.eq(JOB_ID), Mockito.eq(DISPATCH_REVISION),
                Mockito.eq(INVOCATION_ID), Mockito.eq(BE_EPOCH), captor.capture());
        LanceIndexJobResult result = captor.getValue();
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT, result.getResultCode());
        Assertions.assertEquals("commit conflict on the provider", result.getSanitizedMessage());
        Assertions.assertTrue(result.isExternalMetadataAdvanced());
    }

    @Test
    public void masterRecordsAChildReapedProofWithTheDurableBackendId() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        LanceIndexJobManager manager = Mockito.mock(LanceIndexJobManager.class);
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
        // Built before the stubbing, for the same unfinished-stubbing reason.
        LanceIndexJob dispatched = dispatchedJob();
        Mockito.when(manager.getJob(JOB_ID)).thenReturn(dispatched);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStatus status = service.reportLanceIndexJobResult(matchingReport()
                    .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED));
            Assertions.assertEquals(TStatusCode.OK, status.getStatusCode());
        }

        Mockito.verify(manager).completeWithResult(Mockito.eq(JOB_ID), Mockito.eq(DISPATCH_REVISION),
                Mockito.eq(INVOCATION_ID), Mockito.eq(BE_EPOCH), Mockito.any());
        Mockito.verify(manager).recordTerminationProof(Mockito.eq(JOB_ID), Mockito.eq(DISPATCH_REVISION),
                Mockito.eq(BACKEND_ID), Mockito.eq(BE_EPOCH), Mockito.eq(INVOCATION_ID),
                Mockito.eq(LanceIndexTerminationProof.CHILD_REAPED));
    }

    /**
     * The durable record the authentication gate reads: a dispatched RUNNING job
     * whose journaled secret the report echoes. A top-level class, so stubbing its
     * getters on a Mockito mock is safe.
     */
    private static LanceIndexJob dispatchedJob() {
        LanceIndexJob job = Mockito.mock(LanceIndexJob.class);
        Mockito.when(job.getInvocationSecret()).thenReturn(INVOCATION_SECRET);
        Mockito.when(job.getBackendId()).thenReturn(BACKEND_ID);
        return job;
    }

    private static TLanceIndexJobReport matchingReport() {
        return new TLanceIndexJobReport()
                .setJobId(JOB_ID)
                .setDispatchRevision(DISPATCH_REVISION)
                .setInvocationId(INVOCATION_ID)
                .setInvocationSecret(INVOCATION_SECRET)
                .setBeProcessEpoch(BE_EPOCH)
                .setResultCode(TLanceIndexJobResultCode.NATIVE_OK);
    }

    @Test
    public void nonMasterRejectsTheTerminationReportWithoutTouchingTheJobManager() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        LanceIndexJobManager manager = Mockito.mock(LanceIndexJobManager.class);
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStatus status = service.reportLanceIndexJobTermination(matchingTermination());
            Assertions.assertEquals(TStatusCode.NOT_MASTER, status.getStatusCode());
        }
        Mockito.verifyNoInteractions(manager);
    }

    @Test
    public void masterRecordsATerminationProofWithTheDurableBackendId() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        LanceIndexJobManager manager = Mockito.mock(LanceIndexJobManager.class);
        Mockito.when(env.getLanceIndexJobManager()).thenReturn(manager);
        LanceIndexJob dispatched = Mockito.mock(LanceIndexJob.class);
        Mockito.when(dispatched.getBackendId()).thenReturn(BACKEND_ID);
        Mockito.when(manager.getJob(JOB_ID)).thenReturn(dispatched);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TStatus status = service.reportLanceIndexJobTermination(matchingTermination());
            Assertions.assertEquals(TStatusCode.OK, status.getStatusCode());
        }

        // A termination report places a proof only: it never completes the job.
        Mockito.verify(manager, Mockito.never()).completeWithResult(Mockito.anyLong(), Mockito.anyLong(),
                Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(manager).recordTerminationProof(Mockito.eq(JOB_ID), Mockito.eq(DISPATCH_REVISION),
                Mockito.eq(BACKEND_ID), Mockito.eq(BE_EPOCH), Mockito.eq(INVOCATION_ID),
                Mockito.eq(LanceIndexTerminationProof.NEVER_LAUNCHED));
    }

    private static TLanceIndexJobTerminationReport matchingTermination() {
        return new TLanceIndexJobTerminationReport()
                .setJobId(JOB_ID)
                .setDispatchRevision(DISPATCH_REVISION)
                .setInvocationId(INVOCATION_ID)
                .setBeProcessEpoch(BE_EPOCH)
                .setProof(TLanceIndexTerminationProof.NEVER_LAUNCHED);
    }
}
