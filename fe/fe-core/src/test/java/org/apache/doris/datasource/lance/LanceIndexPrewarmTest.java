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
import org.apache.doris.datasource.lance.metadata.LanceIndexPrewarmMetadata;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
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
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.mockito.Mockito;

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

    private LanceIndexPrewarmMetadata metadata(long version) {
        return new LanceIndexPrewarmMetadata(
                new LanceTableAccess("s3://test-bucket/dataset.lance",
                        Collections.singletonMap("aws_session_token", "test-only-token")),
                version, new Schema(Collections.emptyList()), Collections.singletonMap("vector", 0),
                Collections.singletonMap(INDEX, Collections.singletonList(0)));
    }

    @Test
    void discoversLegacyScalarIndexesAlongsideModernIndexes() throws Exception {
        for (boolean mixed : new boolean[] {false, true}) {
            Dataset dataset = Mockito.mock(Dataset.class);
            LanceSchema lanceSchema = Mockito.mock(LanceSchema.class);
            LanceField field = Mockito.mock(LanceField.class);
            Mockito.when(field.getName()).thenReturn("id");
            Mockito.when(field.getId()).thenReturn(3);
            Mockito.when(lanceSchema.fields()).thenReturn(Collections.singletonList(field));
            Mockito.when(dataset.getLanceSchema()).thenReturn(lanceSchema);
            Mockito.when(dataset.getSchema()).thenReturn(new Schema(Collections.singletonList(
                    Field.nullable("id", new ArrowType.Int(64, true)))));
            Mockito.when(dataset.version()).thenReturn(9L);
            Index legacy = Index.builder().name("id").uuid(UUID.randomUUID()).datasetVersion(9)
                    .fields(Collections.singletonList(3)).build();
            Index modern = Index.builder().name("modern").uuid(UUID.randomUUID()).datasetVersion(9)
                    .fields(Collections.singletonList(3)).indexType(IndexType.BTREE)
                    .indexDetails(new byte[] {1}).build();
            Index secondSegment = Index.builder().name("id").uuid(UUID.randomUUID()).datasetVersion(9)
                    .fields(Collections.singletonList(3)).build();
            Index internal = Index.builder().name("__lance_frag_reuse").uuid(UUID.randomUUID()).build();
            Mockito.when(dataset.getIndexes()).thenReturn(mixed
                    ? Arrays.asList(legacy, modern, secondSegment, internal)
                    : Arrays.asList(legacy, secondSegment, internal));
            LanceIndexPrewarmMetadata metadata = LanceMetadataLoader.readForPrewarm(dataset,
                    new LanceTableAccess("s3://test-bucket/legacy.lance", Collections.emptyMap()),
                    LanceMetadataMetrics.disabled());
            Mockito.verify(dataset, Mockito.never()).describeIndices(Mockito.any());
            List<String> expected = mixed ? Arrays.asList("id", "modern") : Collections.singletonList("id");
            Assertions.assertEquals(expected, LanceIndexPrewarm.selectIndexes(metadata, Collections.emptyList()));
            Assertions.assertEquals(expected, LanceIndexPrewarm.selectIndexes(metadata, Collections.singletonList("id")));
            Assertions.assertEquals(9, LanceIndexPrewarm.request(metadata, "id").getDatasetVersion());
            Mockito.verify(dataset, Mockito.never()).listIndexes();
            Mockito.verify(dataset, Mockito.never()).getFragments();
            Mockito.verify(dataset, Mockito.never()).close();
        }
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
        LanceIndexPrewarmMetadata snapshot = new LanceIndexPrewarmMetadata(
                new LanceTableAccess("s3://test-bucket/dataset.lance", Collections.emptyMap()),
                9, schema, ImmutableMap.of("vector", 8, "id", 3, "unindexed", 12),
                ImmutableMap.of(INDEX, Collections.singletonList(8), "id_idx", Collections.singletonList(3)));
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
        LanceIndexPrewarmMetadata unavailable = new LanceIndexPrewarmMetadata(
                new LanceTableAccess("s3://test-bucket/dataset.lance", Collections.emptyMap()),
                9, schema, Collections.emptyMap(), Collections.singletonMap(INDEX, Collections.singletonList(8)));
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
