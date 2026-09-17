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

package org.apache.doris.datasource.doris;

import org.apache.doris.common.GenericPool;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TGetOlapTableMetaRequest;
import org.apache.doris.thrift.TGetOlapTableMetaResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class FeServiceClientTest {
    @Test
    public void testReadTimeoutSecondsReachThriftAsMilliseconds(@Mocked FrontendService.Client frontend)
            throws Exception {
        AtomicInteger borrowedTimeoutMs = new AtomicInteger(-1);
        new MockUp<GenericPool<FrontendService.Client>>() {
            @Mock
            public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) {
                borrowedTimeoutMs.set(timeoutMs);
                return frontend;
            }

            @Mock
            public void returnObject(TNetworkAddress address, FrontendService.Client client) {
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, FrontendService.Client client) {
            }
        };
        new Expectations() {
            {
                frontend.getBackendMeta((TGetBackendMetaRequest) any);
                result = new TGetBackendMetaResult().setBackends(Collections.emptyList());
                frontend.getOlapTableMeta((TGetOlapTableMetaRequest) any);
                result = new TGetOlapTableMetaResult().setStatus(new TStatus(TStatusCode.INTERNAL_ERROR)
                        .setErrorMsgs(Collections.singletonList("metadata unavailable")));
            }
        };
        // Verify the actual pool boundary, including zero's infinite-timeout semantics.
        int[][] cases = {{10, 10000}, {7, 7000}, {0, 0}, {2147483, 2147483000}};
        for (int[] testCase : cases) {
            FeServiceClient client = new FeServiceClient("remote", Collections.singletonList(
                    new TNetworkAddress("localhost", 9020)), "", "", 3, testCase[0]);
            Assert.assertTrue(client.listBackends().isEmpty());
            Assert.assertEquals(testCase[1], borrowedTimeoutMs.get());

            borrowedTimeoutMs.set(-1);
            // Fail after the metadata RPC to isolate its timeout from table deserialization.
            RuntimeException error = Assert.assertThrows(RuntimeException.class,
                    () -> client.getOlapTable("db", "table", 1, Collections.emptyList()));
            Assert.assertTrue(error.getMessage().contains("metadata unavailable"));
            Assert.assertEquals(testCase[1], borrowedTimeoutMs.get());
        }
    }

    @Test
    public void testRejectsInvalidReadTimeoutBeforeOpeningConnection() {
        for (int timeoutSec : new int[] {-1, 2147484, Integer.MAX_VALUE}) {
            Assert.assertThrows(IllegalArgumentException.class, () -> new FeServiceClient("remote",
                    Collections.singletonList(new TNetworkAddress("localhost", 9020)), "", "", 3,
                    timeoutSec));
        }
    }
}
