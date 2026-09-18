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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.rpc.RpcException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class CloudPartitionPendingVersionTest {
    private ConnectContext previousContext;
    private MockedStatic<Env> env;
    private MockedStatic<VersionHelper> versions;
    private CloudPartition partition;
    private Cloud.GetVersionResponse response;
    private Cloud.GetVersionRequest lastRequest;
    private Runnable beforeResponse;
    private int requests;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.getSessionVariable().cloudPartitionVersionCacheTtlMs = Long.MAX_VALUE;
        ctx.setThreadLocalInfo();
        env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentInternalCatalog).thenReturn(Mockito.mock(InternalCatalog.class));
        partition = CloudPartitionTest.createPartition(1, 2, 3);
        partition.setCachedVisibleVersion(12, 1000);
        versions = Mockito.mockStatic(VersionHelper.class);
        versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any(Cloud.GetVersionRequest.class)))
                .thenAnswer(invocation -> nextResponse(invocation.getArgument(0)));
        versions.when(() -> VersionHelper.getVersionFromMeta(
                Mockito.any(Cloud.GetVersionRequest.class), Mockito.anyInt()))
                .thenAnswer(invocation -> nextResponse(invocation.getArgument(0)));
    }

    @AfterEach
    public void tearDown() {
        versions.close();
        env.close();
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPendingReadCannotRestoreInvalidatedCache(boolean batch) throws Exception {
        partition.invalidateCachedVisibleVersion();
        response = response(12).addHasPendingTxns(true).build();
        Assertions.assertEquals(12, read(batch));
        Assertions.assertTrue(partition.isCachedVersionExpired());
        // A later ordinary commit must not hide the pending update by refreshing the cache timestamp.
        partition.setCachedVisibleVersion(12, 2000);
        Assertions.assertEquals(12, read(batch));
        Assertions.assertEquals(2, requests);
        Assertions.assertTrue(partition.isCachedVersionExpired());

        response = response(13).addHasPendingTxns(false).build();
        Assertions.assertEquals(13, read(batch));
        Assertions.assertFalse(partition.isCachedVersionExpired());
        Assertions.assertEquals(13, read(batch));
        Assertions.assertEquals(3, requests, "A non-pending query response may restore caching without a daemon");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPendingReadInvalidatesPreviouslyValidCache(boolean batch) throws Exception {
        Assertions.assertFalse(partition.isCachedVersionExpired());
        ConnectContext.get().getSessionVariable().cloudPartitionVersionCacheTtlMs = 0;
        response = response(12).addHasPendingTxns(true).build();
        Assertions.assertEquals(12, read(batch));
        ConnectContext.get().getSessionVariable().cloudPartitionVersionCacheTtlMs = Long.MAX_VALUE;
        Assertions.assertTrue(partition.isCachedVersionExpired());
    }

    @Test
    public void testBatchInvalidatesOnlyPendingPartitions() throws Exception {
        CloudPartition other = CloudPartitionTest.createPartition(4, 2, 3);
        other.setCachedVisibleVersion(7, 1000);
        response = response(12).addVersions(8).addHasPendingTxns(true).addHasPendingTxns(false).build();
        // Forced MS reads may observe pending while the previous cache was still valid.
        Assertions.assertEquals(List.of(12L, 8L),
                CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition, other), false));
        Assertions.assertTrue(partition.isCachedVersionExpired());
        Assertions.assertFalse(other.isCachedVersionExpired());
        response = response(13).addHasPendingTxns(false).build();
        Assertions.assertEquals(List.of(13L, 8L),
                CloudPartition.getSnapshotVisibleVersion(List.of(partition, other)));
        Assertions.assertFalse(partition.isCachedVersionExpired());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFirstPendingWriteDoesNotCacheEmptyPartition(boolean batch) throws Exception {
        partition = CloudPartitionTest.createPartition(1, 2, 3);
        response = response(-1).setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                .setCode(batch ? MetaServiceCode.OK : MetaServiceCode.VERSION_NOT_FOUND))
                .addHasPendingTxns(true).build();
        Assertions.assertEquals(1, read(batch));
        Assertions.assertTrue(partition.isCachedVersionExpired());
        response = response(2).addHasPendingTxns(false).build();
        Assertions.assertEquals(2, read(batch));
        Assertions.assertFalse(partition.isCachedVersionExpired());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNonPendingResponsePreservesConcurrentInvalidation(boolean batch) throws Exception {
        partition.invalidateCachedVisibleVersion();
        response = response(13).addHasPendingTxns(false).build();
        beforeResponse = partition::invalidateCachedVisibleVersion;
        Assertions.assertEquals(13, read(batch));
        Assertions.assertTrue(partition.isCachedVersionExpired());
        beforeResponse = null;
        Assertions.assertEquals(13, read(batch));
        Assertions.assertFalse(partition.isCachedVersionExpired());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOlderMsPreservesLegacyCacheBehavior(boolean batch) throws Exception {
        partition.invalidateCachedVisibleVersion();
        response = response(13).build();
        Assertions.assertEquals(13, read(batch));
        Assertions.assertFalse(partition.isCachedVersionExpired());
        Assertions.assertEquals(13, read(batch));
        Assertions.assertEquals(1, requests, "An older MS response must still allow cache hits without waiting");

        partition.invalidateCachedVisibleVersion();
        beforeResponse = partition::invalidateCachedVisibleVersion;
        Assertions.assertEquals(13, read(batch));
        Assertions.assertTrue(partition.isCachedVersionExpired(),
                "Legacy compatibility must not acknowledge invalidation after the RPC started");
    }

    @Test
    public void testExplicitPendingOverridesWaitFlag() throws Exception {
        response = response(12).addHasPendingTxns(true).build();
        CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition), true);
        Assertions.assertTrue(partition.isCachedVersionExpired());
        response = response(13).addHasPendingTxns(false).build();
        CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition), true);
        Assertions.assertFalse(partition.isCachedVersionExpired());
    }

    @ParameterizedTest
    @CsvSource({"false,false,false", "false,true,false", "true,false,false", "true,true,false",
            "false,false,true", "false,true,true", "true,false,true", "true,true,true"})
    public void testSessionWaitFlagPreservesCachePolicy(
            boolean batch, boolean waitForPendingTxns, boolean cacheEnabled) throws Exception {
        SessionVariable session = ConnectContext.get().getSessionVariable();
        session.cloudGetVersionWaitForPendingTxn = waitForPendingTxns;
        session.cloudPartitionVersionCacheTtlMs = cacheEnabled ? Long.MAX_VALUE : 0;
        response = response(13).addHasPendingTxns(false).build();
        if (cacheEnabled) {
            Assertions.assertEquals(12, read(batch));
            Assertions.assertEquals(0, requests, "Enabling waiting must not bypass a valid cache");
            partition.invalidateCachedVisibleVersion();
        }
        Assertions.assertEquals(13, read(batch));
        Assertions.assertEquals(1, requests);
        Assertions.assertEquals(batch, lastRequest.getBatchMode());
        Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
        Assertions.assertEquals(13, read(batch));
        Assertions.assertEquals(cacheEnabled ? 1 : 2, requests);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testForcedReadUsesSessionWaitFlag(boolean waitForPendingTxns) throws Exception {
        ConnectContext.get().getSessionVariable().cloudGetVersionWaitForPendingTxn = waitForPendingTxns;
        response = response(13).addHasPendingTxns(false).build();
        Assertions.assertEquals(List.of(13L), CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition)));
        Assertions.assertEquals(1, requests);
        Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExplicitWaitFlagOverridesSession(boolean waitForPendingTxns) throws Exception {
        ConnectContext.get().getSessionVariable().cloudGetVersionWaitForPendingTxn = !waitForPendingTxns;
        response = response(13).addHasPendingTxns(false).build();
        Assertions.assertEquals(13, partition.getVisibleVersionFromMs(waitForPendingTxns));
        Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
        Assertions.assertEquals(List.of(13L),
                CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition), waitForPendingTxns));
        Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReadWithoutSessionUsesGlobalWaitFlag(boolean waitForPendingTxns) throws Exception {
        SessionVariable defaults = VariableMgr.getDefaultSessionVariable();
        boolean previousWait = defaults.cloudGetVersionWaitForPendingTxn;
        try {
            defaults.cloudGetVersionWaitForPendingTxn = waitForPendingTxns;
            ConnectContext.get().getSessionVariable().cloudGetVersionWaitForPendingTxn = !waitForPendingTxns;
            response = response(13).addHasPendingTxns(false).build();
            CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition));
            Assertions.assertEquals(!waitForPendingTxns, lastRequest.getWaitForPendingTxn());

            ConnectContext.remove();
            Assertions.assertEquals(List.of(13L), CloudPartition.getSnapshotVisibleVersionFromMs(List.of(partition)));
            Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
            partition.invalidateCachedVisibleVersion();
            Assertions.assertEquals(13, partition.getVisibleVersion());
            Assertions.assertEquals(waitForPendingTxns, lastRequest.getWaitForPendingTxn());
        } finally {
            defaults.cloudGetVersionWaitForPendingTxn = previousWait;
        }
    }

    private long read(boolean batch) throws RpcException {
        return batch ? CloudPartition.getSnapshotVisibleVersion(List.of(partition)).get(0)
                : partition.getVisibleVersion();
    }

    private Cloud.GetVersionResponse nextResponse(Cloud.GetVersionRequest request) {
        lastRequest = request;
        requests++;
        if (beforeResponse != null) {
            beforeResponse.run();
        }
        return response;
    }

    private Cloud.GetVersionResponse.Builder response(long version) {
        return Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(MetaServiceCode.OK))
                .setVersion(version).addVersions(version).addVersionUpdateTimeMs(2000).addCommitTsos(3000);
    }
}
