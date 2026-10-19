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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.index.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.proto.InternalService.PLanceIndexPrewarmRequest;
import org.apache.doris.proto.InternalService.PLanceIndexPrewarmResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class LanceIndexPrewarmTest {
    private static final String INDEX = "vector_idx";

    private LanceTableMetadata metadata(long version) {
        LanceIndexSegmentInfo segment = new LanceIndexSegmentInfo(UUID.randomUUID(), INDEX,
                Collections.singletonList(0), Collections.singletonList(0L), IndexType.IVF_PQ, "l2");
        return LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://test-bucket/dataset.lance",
                        Collections.singletonMap("aws_session_token", "test-only-token")),
                version, new Schema(Collections.emptyList()), Collections.emptyList(),
                Collections.singletonMap("vector", 0), Arrays.asList(segment, segment));
    }

    private PLanceIndexPrewarmRequest request() throws Exception {
        return LanceIndexPrewarm.request(metadata(7), INDEX);
    }

    private PLanceIndexPrewarmResponse success() {
        return PLanceIndexPrewarmResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0))
                .setDatasetVersion(7).build();
    }

    private List<Backend> backends(int count) {
        List<Backend> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Backend backend = new Backend(i + 1, "127.0.0.1", 9050 + i);
            backend.setAlive(true);
            result.add(backend);
        }
        return result;
    }

    private long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    }

    @Test
    void selectsIndexesBySchemaFieldIdsAndDeduplicatesSegments() throws Exception {
        Schema schema = new Schema(Arrays.asList(
                new Field("vector", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("unindexed", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        LanceIndexSegmentInfo vector = new LanceIndexSegmentInfo(UUID.randomUUID(), INDEX,
                Collections.singletonList(8), Collections.singletonList(0L), IndexType.IVF_PQ, "l2");
        LanceIndexSegmentInfo scalar = new LanceIndexSegmentInfo(UUID.randomUUID(), "id_idx",
                Collections.singletonList(3), Collections.singletonList(0L), IndexType.BTREE, null);
        LanceTableMetadata snapshot = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://test-bucket/dataset.lance", Collections.emptyMap()),
                9, schema, Collections.emptyList(), ImmutableMap.of("vector", 8, "id", 3, "unindexed", 12),
                Arrays.asList(vector, scalar, vector));
        Assertions.assertEquals(Arrays.asList(INDEX, "id_idx"),
                LanceIndexPrewarm.selectIndexes(snapshot, Collections.emptyList()));
        Assertions.assertEquals(Collections.singletonList(INDEX),
                LanceIndexPrewarm.selectIndexes(snapshot, Arrays.asList("VECTOR", "vector")));
        Assertions.assertEquals(Collections.singletonList("id_idx"),
                LanceIndexPrewarm.selectIndexes(snapshot, Collections.singletonList("id")));
        Assertions.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.selectIndexes(snapshot, Arrays.asList("vector", "missing")));
        Assertions.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.selectIndexes(snapshot, Collections.singletonList("unindexed")));
        LanceTableMetadata unavailable = LanceTableMetadata.createSnapshotWithUnavailableFieldIds(
                new LanceTableAccess("s3://test-bucket/dataset.lance", Collections.emptyMap()),
                9, schema, Collections.emptyList(), Collections.singletonList(vector));
        Assertions.assertEquals(Collections.singletonList(INDEX),
                LanceIndexPrewarm.selectIndexes(unavailable, Collections.emptyList()));
        Assertions.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.selectIndexes(unavailable, Collections.singletonList("vector")));
    }

    @Test
    void pinsVersionLogicalIndexAndVendedCredentials() throws Exception {
        PLanceIndexPrewarmRequest request = request();
        Assertions.assertEquals(7, request.getDatasetVersion());
        Assertions.assertEquals(INDEX, request.getIndexName());
        Assertions.assertEquals("test-only-token", request.getStorageOptionsOrThrow("aws_session_token"));
        Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.request(metadata(0), INDEX));
        Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.request(metadata(7), "missing"));
    }

    @Test
    void waitsForEveryBackendAndBoundsFanout() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger sent = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        PLanceIndexPrewarmRequest request = request();
        LanceIndexPrewarm.execute(backends(19), request, deadline(), () -> false, (backend, rpc, timeout) -> {
            Assertions.assertTrue(active.incrementAndGet() <= LanceIndexPrewarm.MAX_IN_FLIGHT);
            Assertions.assertEquals(request.getDatasetVersion(), rpc.getDatasetVersion());
            Assertions.assertEquals(request.getStorageOptionsMap(), rpc.getStorageOptionsMap());
            Assertions.assertEquals(timeout, rpc.getTimeoutMs());
            sent.incrementAndGet();
            return new CompletableFuture<PLanceIndexPrewarmResponse>() {
                @Override
                public PLanceIndexPrewarmResponse get(long timeout, TimeUnit unit) {
                    active.decrementAndGet();
                    completed.incrementAndGet();
                    return success();
                }
            };
        });
        Assertions.assertEquals(19, sent.get());
        Assertions.assertEquals(19, completed.get());
        Assertions.assertEquals(0, active.get());
    }

    @Test
    void rejectsMissingAcknowledgementAndWrongSnapshot() throws Exception {
        List<PLanceIndexPrewarmResponse> invalid = Arrays.asList(
                PLanceIndexPrewarmResponse.getDefaultInstance(),
                PLanceIndexPrewarmResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0)).build(),
                success().toBuilder().setDatasetVersion(8).build(),
                success().toBuilder().setStatus(PStatus.newBuilder().setStatusCode(1)).build());
        for (PLanceIndexPrewarmResponse response : invalid) {
            UserException error = Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.execute(
                    backends(2), request(), deadline(), () -> false,
                    (backend, rpc, timeout) -> CompletableFuture.completedFuture(response)));
            Assertions.assertTrue(error.getMessage().contains("backend 1"));
            Assertions.assertTrue(error.getMessage().contains(INDEX));
        }
    }

    @Test
    void failureCancelsOutstandingCallsWithoutLeakingProviderMessages() throws Exception {
        CompletableFuture<PLanceIndexPrewarmResponse> pending = new CompletableFuture<>();
        UserException error = Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.execute(
                backends(2), request(), deadline(), () -> false, (backend, rpc, timeout) -> {
                    if (backend.getId() == 2) {
                        throw new IllegalStateException("test-only-token s3://test-bucket/dataset.lance");
                    }
                    return pending;
                }));
        Assertions.assertTrue(pending.isCancelled());
        Assertions.assertTrue(error.getMessage().contains("backend 2"));
        Assertions.assertFalse(error.getMessage().contains("test-only-token"));
        Assertions.assertFalse(error.getMessage().contains("s3://"));
    }

    @Test
    void timeoutAndCancellationStopWaiting() throws Exception {
        CompletableFuture<PLanceIndexPrewarmResponse> pending = new CompletableFuture<>();
        AtomicInteger sent = new AtomicInteger();
        UserException cancelled = Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.execute(
                backends(1), request(), deadline(), () -> sent.get() > 0, (backend, rpc, timeout) -> {
                    sent.incrementAndGet();
                    return pending;
                }));
        Assertions.assertTrue(cancelled.getMessage().contains("cancelled"));
        Assertions.assertTrue(pending.isCancelled());
        UserException timeout = Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.execute(
                backends(1), request(), System.nanoTime(), () -> false, (backend, rpc, timeoutMs) -> {
                    Assertions.fail("Expired statements must not dispatch RPCs");
                    return pending;
                }));
        Assertions.assertTrue(timeout.getMessage().contains("timed out"));
    }

    @Test
    void enforcesOneDeadlineWhileRpcRemainsPending() throws Exception {
        CompletableFuture<PLanceIndexPrewarmResponse> pending = new CompletableFuture<>();
        UserException error = Assertions.assertThrows(UserException.class, () -> LanceIndexPrewarm.execute(
                backends(1), request(), System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50), () -> false,
                (backend, rpc, timeout) -> pending));
        Assertions.assertTrue(error.getMessage().contains("timed out"));
        Assertions.assertTrue(pending.isCancelled());
    }

    @Test
    void selectsAllEligibleBackendsFromOnlyTheResolvedGroup() throws Exception {
        List<Backend> groupBackends = backends(3);
        groupBackends.get(2).setAlive(false);
        ComputeGroup group = new ComputeGroup("selected", "selected", null) {
            @Override
            public List<Backend> getBackendList() {
                return groupBackends;
            }
        };
        List<Backend> selected = LanceIndexPrewarm.selectBackends(group);
        Assertions.assertEquals(2, selected.size());
        Assertions.assertTrue(selected.containsAll(groupBackends.subList(0, 2)));
        Assertions.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.selectBackends(ComputeGroup.INVALID_COMPUTE_GROUP));
    }
}
