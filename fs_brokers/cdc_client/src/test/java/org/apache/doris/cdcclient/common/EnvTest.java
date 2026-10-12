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

package org.apache.doris.cdcclient.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.doris.cdcclient.exception.CommonException;
import org.apache.doris.cdcclient.service.PipelineCoordinator;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;

class EnvTest {

    @Test
    void tvfRequestReplacesReaderOwnedByPreviousTask() throws Exception {
        Env env = Env.getCurrentEnv();
        PipelineCoordinator coordinator = new PipelineCoordinator(1);
        Method closeTvfReader =
                PipelineCoordinator.class.getDeclaredMethod(
                        "closeTvfReader", FetchRecordRequest.class, SourceReader.class);
        closeTvfReader.setAccessible(true);
        FetchRecordRequest firstRequest = tvfRequest("68168001", "first");
        FetchRecordRequest secondRequest = tvfRequest("68168001", "second");
        SourceReader first = env.getReaderAndClaim(firstRequest, firstRequest.getTaskId());
        try {
            SourceReader second = env.getReaderAndClaim(secondRequest, secondRequest.getTaskId());
            assertNotSame(first, second);
            closeTvfReader.invoke(coordinator, firstRequest, first);
            assertSame(second, env.getReaderIfPresent(secondRequest.getJobId()));
            closeTvfReader.invoke(coordinator, secondRequest, second);
            assertNull(env.getReaderIfPresent(secondRequest.getJobId()));
        } finally {
            SourceReader reader = env.getReaderIfPresent(secondRequest.getJobId());
            if (reader != null) {
                reader.release(secondRequest);
            }
            env.close(secondRequest.getJobId());
        }
    }

    @Test
    void failedTvfPreparationRemovesClaimedReader() {
        Env env = Env.getCurrentEnv();
        FetchRecordRequest request = tvfRequest("68168002", "task");
        try {
            CommonException exception =
                    assertThrows(
                            CommonException.class,
                            () -> new PipelineCoordinator(1).fetchRecordStream(request));
            assertEquals("miss meta offset", exception.getCause().getMessage());
            assertNull(env.getReaderIfPresent(request.getJobId()));
        } finally {
            SourceReader reader = env.getReaderIfPresent(request.getJobId());
            if (reader != null) {
                reader.release(request);
            }
            env.close(request.getJobId());
        }
    }

    @Test
    void fromToStillReusesReaderUnlessRebuildRequested() {
        Env env = Env.getCurrentEnv();
        WriteRecordRequest request = new WriteRecordRequest();
        request.setJobId("from-to-reader-reuse");
        request.setDataSource("POSTGRES");
        request.setConfig(Collections.emptyMap());
        SourceReader first = env.getReaderAndClaim(request, "first");
        try {
            assertSame(first, env.getReaderAndClaim(request, "second"));
            request.setRebuildReader(true);
            assertNotSame(first, env.getReaderAndClaim(request, "third"));
        } finally {
            env.getReaderIfPresent(request.getJobId()).release(request);
            first.release(request);
            env.close(request.getJobId());
        }
    }

    @Test
    void getReaderIfPresentReturnsNullForUnknownJob() {
        // An off-target releaseReader RPC must be a no-op, never create a reader -> peek returns null.
        assertNull(Env.getCurrentEnv().getReaderIfPresent("no-such-job-id"));
    }

    @Test
    void detachReaderIfOwnerReturnsNullForUnknownJob() {
        // Stale release for an unknown job (no lock/context) must be a no-op.
        assertNull(Env.getCurrentEnv().detachReaderIfOwner("no-such-job-id", "t1"));
    }

    private FetchRecordRequest tvfRequest(String jobId, String taskId) {
        FetchRecordRequest request = new FetchRecordRequest();
        request.setJobId(jobId);
        request.setTaskId(taskId);
        request.setDataSource("POSTGRES");
        request.setConfig(Collections.emptyMap());
        return request;
    }
}
