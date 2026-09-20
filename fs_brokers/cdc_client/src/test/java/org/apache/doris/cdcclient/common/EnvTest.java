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

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import org.junit.jupiter.api.Test;

import java.util.Collections;

class EnvTest {

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
}
