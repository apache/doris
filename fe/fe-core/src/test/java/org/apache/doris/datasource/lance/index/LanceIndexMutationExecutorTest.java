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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceIndexMutationOutcome;
import org.apache.doris.datasource.lance.LanceIndexSchemaContract;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The classification and admission matrix of {@link LanceIndexMutationExecutor}, all through
 * the {@link LanceIndexMutationDispatcher} seam (no client pool, no live backend): every
 * backend answer family maps to its outcome kind, pre-dispatch rejections never send, the
 * same-name busy guard fails fast, admission capacity is bounded, and the active-name entry
 * is released on every path including exceptions - plus a concurrency pass.
 */
public class LanceIndexMutationExecutorTest {

    private static final long CATALOG_ID = 10L;
    private static final String DATASET_URI = "s3://bucket/dataset";
    private static final long DATASET_VERSION = 7L;
    private static final long BE_PROCESS_EPOCH = 42L;
    private static final long BE_ID = 1L;
    private static final long DEFAULT_DEADLINE_MS = System.currentTimeMillis() + 60_000L;

    private LanceExternalCatalog catalog;
    private SystemInfoService systemInfo;
    private Backend backend;
    private BooleanSupplier originalMasterCheck;
    private Supplier<SystemInfoService> originalBackendInventory;

    /** The pluggable answer of the fake dispatcher: one dispatch in, one result or throw out. */
    private interface Answer {
        TLanceIndexMutationResult answer(TLanceIndexMutationRequest request) throws Exception;
    }

    private static final class FakeDispatcher implements LanceIndexMutationDispatcher {
        private final Answer answer;
        final AtomicInteger sends = new AtomicInteger();
        final List<TLanceIndexMutationRequest> requests = new CopyOnWriteArrayList<>();

        FakeDispatcher(Answer answer) {
            this.answer = answer;
        }

        @Override
        public TLanceIndexMutationResult dispatch(TLanceIndexMutationRequest request, Backend recipient,
                long timeoutMillis) throws Exception {
            sends.incrementAndGet();
            requests.add(request);
            return answer.answer(request);
        }
    }

    @BeforeEach
    public void setUp() {
        catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getOrderedStoragePropertiesList())
                .thenReturn(Collections.emptyList());
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(BE_ID);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getBePort()).thenReturn(9020);
        Mockito.when(backend.getProcessEpoch()).thenReturn(BE_PROCESS_EPOCH);

        systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllBackendIds(false)).thenReturn(Collections.singletonList(BE_ID));
        Mockito.when(systemInfo.getBackend(BE_ID)).thenReturn(backend);
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(), Mockito.anyInt()))
                .thenReturn(Collections.singletonList(BE_ID));

        originalMasterCheck = LanceIndexMutationExecutor.masterCheck;
        originalBackendInventory = LanceIndexMutationExecutor.backendInventory;
        LanceIndexMutationExecutor.masterCheck = () -> true;
        LanceIndexMutationExecutor.backendInventory = () -> systemInfo;
    }

    @AfterEach
    public void tearDown() {
        LanceIndexMutationExecutor.masterCheck = originalMasterCheck;
        LanceIndexMutationExecutor.backendInventory = originalBackendInventory;
    }

    // ---------------------------------------------------------------- classification matrix

    @Test
    public void testNativeSuccessClassifiesSuccessAndSendsOnce() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS, outcome.getKind());
        Assertions.assertEquals(0, outcome.getLanceResultCode());
        Assertions.assertEquals(1, dispatcher.sends.get());
        Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
    }

    @Test
    public void testCleanNonOkStatusIsConfirmedFailure() throws Exception {
        TStatus status = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);
        status.setErrorMsgs(Collections.singletonList("lance index worker is not available in this build"));
        FakeDispatcher dispatcher = new FakeDispatcher(
                request -> new TLanceIndexMutationResult().setStatus(status));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED,
                outcome.toUserException().getMysqlErrorCode());
        // The message text is diagnostics only - the classification above read the status code.
        Assertions.assertTrue(outcome.getMessage().contains("NOT_IMPLEMENTED_ERROR"));
    }

    @Test
    public void testTypedCommitConflictIsConfirmedFailure() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(
                request -> okResult(LanceIndexMutationExecutor.LANCE_ERR_COMMIT_CONFLICT));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertEquals(LanceIndexMutationExecutor.LANCE_ERR_COMMIT_CONFLICT,
                outcome.getLanceResultCode());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED,
                outcome.toUserException().getMysqlErrorCode());
    }

    @Test
    public void testOtherTypedCodeAfterInvocationIsIndeterminate() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(2));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
        Assertions.assertEquals(2, outcome.getLanceResultCode());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_INDETERMINATE,
                outcome.toUserException().getMysqlErrorCode());
        Assertions.assertTrue(outcome.getMessage().contains("cannot prove"));
    }

    @Test
    public void testOkStatusWithoutTypedCodeIsIndeterminatePartialAnswer() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(
                request -> new TLanceIndexMutationResult().setStatus(new TStatus(TStatusCode.OK)));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
        Assertions.assertTrue(outcome.getMessage().contains("no typed lance result code"));
    }

    @Test
    public void testNullStatusIsIndeterminatePartialAnswer() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> new TLanceIndexMutationResult());
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
    }

    @Test
    public void testUnknownMethodIsConfirmedFailure() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            throw new TApplicationException(TApplicationException.UNKNOWN_METHOD,
                    "Invalid method name: lance_index_mutate");
        });
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertTrue(outcome.getMessage().contains("rolling upgrade"));
    }

    @Test
    public void testOtherApplicationExceptionIsIndeterminate() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            throw new TApplicationException(TApplicationException.INTERNAL_ERROR, "protocol chaos");
        });
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
    }

    @Test
    public void testReadTimeoutExceptionIsIndeterminate() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            throw new TTransportException(TTransportException.TIMED_OUT, "read timeout");
        });
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_INDETERMINATE,
                outcome.toUserException().getMysqlErrorCode());
        Assertions.assertTrue(outcome.getMessage().contains("may or may not have committed"));
    }

    @Test
    public void testConnectionExceptionIsIndeterminate() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            throw new TTransportException(TTransportException.NOT_OPEN, "connection closed");
        });
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
        Assertions.assertEquals(1, dispatcher.sends.get());
    }

    // ---------------------------------------------------------------- pre-dispatch rejections

    @Test
    public void testBudgetExhaustedBeforeDispatchIsConfirmedFailureWithoutSend() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", System.currentTimeMillis() - 1_000L)
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .build();
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, request);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        // The message must distinguish a budget exhaustion that predates the dispatch.
        Assertions.assertTrue(outcome.getMessage().contains("before the dispatch"));
        Assertions.assertEquals(0, dispatcher.sends.get());
    }

    @Test
    public void testNoAliveBackendIsConfirmedFailureWithoutSend() throws Exception {
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(), Mockito.anyInt()))
                .thenReturn(Collections.emptyList());
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, createRequest());
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertTrue(outcome.getMessage().contains("no alive backend"));
        Assertions.assertEquals(0, dispatcher.sends.get());
    }

    @Test
    public void testCancelledBeforeDispatchIsConfirmedFailureWithoutSend() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", DEFAULT_DEADLINE_MS)
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setCancelSignal(() -> true)
                .build();
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, request);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertTrue(outcome.getMessage().contains("cancelled"));
        Assertions.assertEquals(0, dispatcher.sends.get());
    }

    @Test
    public void testNoOpPreflightCompletesWithoutDispatchEvenWithNoBudgetLeft() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        // The budget is already gone; a no-op never dispatches, so it must still complete.
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", System.currentTimeMillis() - 1_000L)
                .setFinalPreflight(() -> LanceIndexMutationExecutor.PreflightResult.NO_OP)
                .build();
        LanceIndexMutationOutcome outcome = executeWithDirectExecutor(dispatcher, request);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS, outcome.getKind());
        Assertions.assertEquals(0, dispatcher.sends.get());
        Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
    }

    @Test
    public void testPreflightRejectionPropagatesItsOwnTypedErrorAndReleasesEntry() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", DEFAULT_DEADLINE_MS)
                .setFinalPreflight(() -> {
                    throw new AnalysisException(
                            ErrorCode.ERR_LANCE_INDEX_INVALID.formatErrorMsg("index 'idx' already exists"),
                            ErrorCode.ERR_LANCE_INDEX_INVALID);
                })
                .build();
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> executeWithDirectExecutor(dispatcher, request));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_INVALID,
                exception.getMysqlErrorCode());
        // The entry must be released even though the phases threw.
        Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
        Assertions.assertEquals(0, dispatcher.sends.get());
        // And a same-name follow-up is not busy.
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS,
                executeWithDirectExecutor(dispatcher, createRequest()).getKind());
    }

    // ---------------------------------------------------------------- busy guard and admission

    @Test
    public void testSameNameInFlightFailsFastBusyWithoutQueueing() throws Exception {
        CountDownLatch dispatchEntered = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            dispatchEntered.countDown();
            Assertions.assertTrue(releaseDispatch.await(30, TimeUnit.SECONDS));
            return okResult(0);
        });
        ExecutorService admission = Executors.newFixedThreadPool(1);
        ExecutorService drivers = Executors.newSingleThreadExecutor();
        try {
            Future<LanceIndexMutationOutcome> first = drivers.submit(
                    () -> LanceIndexMutationExecutor.execute(createRequest(), dispatcher, admission));
            Assertions.assertTrue(dispatchEntered.await(30, TimeUnit.SECONDS));
            // While the first statement holds the entry, a same-name statement fails fast with
            // the typed busy error - it must not queue behind the running one.
            AnalysisException busy = Assertions.assertThrows(AnalysisException.class,
                    () -> LanceIndexMutationExecutor.execute(createRequest(), dispatcher, admission));
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY,
                    busy.getMysqlErrorCode());
            Assertions.assertTrue(busy.getMessage().contains("not queued"));
            Assertions.assertEquals(1, LanceIndexMutationExecutor.inFlightCount());
            releaseDispatch.countDown();
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS,
                    first.get(30, TimeUnit.SECONDS).getKind());
            Assertions.assertEquals(1, dispatcher.sends.get());
            Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
            // Entry released: a same-name statement is admitted again.
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS,
                    executeWithDirectExecutor(dispatcher, createRequest()).getKind());
        } finally {
            releaseDispatch.countDown();
            drivers.shutdownNow();
            admission.shutdownNow();
        }
    }

    @Test
    public void testSameNameGuardKeyNormalization() {
        // Trailing-slash-only and surrounding-whitespace differences are the same dataset.
        Assertions.assertEquals(LanceIndexMutationExecutor.normalizeDatasetUri("s3://b/ds/"),
                LanceIndexMutationExecutor.normalizeDatasetUri(" s3://b/ds "));
        // Case is significant (object-store buckets and keys are), so it must not be folded.
        Assertions.assertNotEquals(LanceIndexMutationExecutor.normalizeDatasetUri("s3://B/ds"),
                LanceIndexMutationExecutor.normalizeDatasetUri("s3://b/ds"));
    }

    @Test
    public void testFullPoolAndQueueFailsFastBusy() throws Exception {
        CountDownLatch dispatchEntered = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            dispatchEntered.countDown();
            Assertions.assertTrue(releaseDispatch.await(30, TimeUnit.SECONDS));
            return okResult(0);
        });
        // One worker, one queue slot: the running statement occupies the worker, one queued
        // statement fills the queue, the third must fail fast with the typed busy error.
        ThreadPoolExecutor admission = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
        ExecutorService drivers = Executors.newFixedThreadPool(2);
        try {
            Future<LanceIndexMutationOutcome> running = drivers.submit(
                    () -> LanceIndexMutationExecutor.execute(requestBuilder("running").build(),
                            dispatcher, admission));
            Assertions.assertTrue(dispatchEntered.await(30, TimeUnit.SECONDS));
            // The queued statement has no budget left, so its statement-side wait ends
            // immediately in the indeterminate class (it cannot know whether its entry task
            // dispatched); the task itself later converges as a confirmed failure without a send.
            Future<LanceIndexMutationOutcome> queued = drivers.submit(
                    () -> LanceIndexMutationExecutor.execute(requestBuilder("queued", -1L).build(),
                            dispatcher, admission));
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE,
                    queued.get(1, TimeUnit.SECONDS).getKind());
            AnalysisException busy = Assertions.assertThrows(AnalysisException.class,
                    () -> LanceIndexMutationExecutor.execute(requestBuilder("rejected").build(),
                            dispatcher, admission));
            Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY,
                    busy.getMysqlErrorCode());
            Assertions.assertTrue(busy.getMessage().contains("capacity"));
            // The rejected statement released its own entry; the running and queued ones hold theirs.
            Assertions.assertEquals(2, LanceIndexMutationExecutor.inFlightCount());
            releaseDispatch.countDown();
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS,
                    running.get(30, TimeUnit.SECONDS).getKind());
            Assertions.assertTrue(awaitInFlightCount(0), "every entry must be released");
        } finally {
            releaseDispatch.countDown();
            drivers.shutdownNow();
            admission.shutdownNow();
        }
    }

    // ---------------------------------------------------------------- single-send and liveness

    @Test
    public void testStatementBudgetExpiryDuringWaitIsIndeterminateAndNeverResends() throws Exception {
        CountDownLatch taskFinished = new CountDownLatch(1);
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            // The backend answers after the statement budget is gone.
            Thread.sleep(1_500L);
            taskFinished.countDown();
            return okResult(0);
        });
        ExecutorService admission = Executors.newFixedThreadPool(1);
        try {
            LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                    .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                            DATASET_VERSION, "idx", System.currentTimeMillis() + 300L)
                    .setColumnName("v")
                    .setIndexType("IVF_PQ")
                    .build();
            long started = System.currentTimeMillis();
            LanceIndexMutationOutcome outcome =
                    LanceIndexMutationExecutor.execute(request, dispatcher, admission);
            Assertions.assertTrue(System.currentTimeMillis() - started < 1_400L,
                    "the statement must not wait past its budget");
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE, outcome.getKind());
            Assertions.assertTrue(outcome.getMessage().contains("budget elapsed"));
            // The abandoned task still converges alone and releases the entry; it never sends
            // a second time.
            Assertions.assertTrue(taskFinished.await(30, TimeUnit.SECONDS));
            Assertions.assertTrue(awaitInFlightCount(0));
            Assertions.assertEquals(1, dispatcher.sends.get());
        } finally {
            admission.shutdownNow();
        }
    }

    @Test
    public void testConcurrentSameNameExactlyOneDispatchRestBusy() throws Exception {
        CountDownLatch dispatchEntered = new CountDownLatch(1);
        CountDownLatch releaseDispatch = new CountDownLatch(1);
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            dispatchEntered.countDown();
            Assertions.assertTrue(releaseDispatch.await(30, TimeUnit.SECONDS));
            return okResult(0);
        });
        ExecutorService admission = Executors.newFixedThreadPool(2);
        ExecutorService drivers = Executors.newFixedThreadPool(8);
        try {
            List<Future<Object>> results = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(drivers.submit((Callable<Object>) () -> {
                    try {
                        return LanceIndexMutationExecutor.execute(createRequest(), dispatcher, admission);
                    } catch (AnalysisException e) {
                        return e;
                    }
                }));
            }
            Assertions.assertTrue(dispatchEntered.await(30, TimeUnit.SECONDS));
            int busy = 0;
            Future<Object> winner = null;
            for (Future<Object> result : results) {
                try {
                    Object value = result.get(200, TimeUnit.MILLISECONDS);
                    Assertions.assertInstanceOf(AnalysisException.class, value);
                    Assertions.assertEquals(
                            ErrorCode.ERR_LANCE_INDEX_MUTATION_BUSY,
                            ((AnalysisException) value).getMysqlErrorCode());
                    busy++;
                } catch (TimeoutException e) {
                    // The one statement holding the entry is still waiting for its answer.
                    winner = result;
                }
            }
            Assertions.assertEquals(7, busy);
            Assertions.assertNotNull(winner);
            Assertions.assertEquals(1, LanceIndexMutationExecutor.inFlightCount());
            releaseDispatch.countDown();
            Object winnerResult = winner.get(30, TimeUnit.SECONDS);
            Assertions.assertInstanceOf(LanceIndexMutationOutcome.class, winnerResult);
            Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS,
                    ((LanceIndexMutationOutcome) winnerResult).getKind());
            Assertions.assertEquals(1, dispatcher.sends.get());
            Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
        } finally {
            releaseDispatch.countDown();
            drivers.shutdownNow();
            admission.shutdownNow();
        }
    }

    @Test
    public void testEveryOutcomePathLeavesNoEntryBehindUnderLoad() throws Exception {
        // A concurrency pass across mixed faulting answers: transport exceptions, clean
        // rejections and successes must all release their entries.
        FakeDispatcher dispatcher = new FakeDispatcher(request -> {
            String name = request.getIndexName();
            if (name.endsWith("transport")) {
                throw new TTransportException(TTransportException.TIMED_OUT, "boom");
            }
            if (name.endsWith("rejected")) {
                return new TLanceIndexMutationResult().setStatus(
                        new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR));
            }
            return okResult(0);
        });
        ExecutorService drivers = Executors.newFixedThreadPool(3);
        try {
            List<Future<LanceIndexMutationOutcome>> outcomes = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 12; i++) {
                final String name = "idx" + (i % 3 == 0 ? "transport" : i % 3 == 1 ? "rejected" : "ok") + i;
                outcomes.add(drivers.submit(
                        () -> executeWithDirectExecutor(dispatcher, requestBuilder(name).build())));
            }
            for (Future<LanceIndexMutationOutcome> outcome : outcomes) {
                outcome.get(30, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(0, LanceIndexMutationExecutor.inFlightCount());
        } finally {
            drivers.shutdownNow();
        }
    }

    @Test
    public void testNonMasterIsRejected() {
        LanceIndexMutationExecutor.masterCheck = () -> false;
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        Assertions.assertThrows(IllegalStateException.class,
                () -> executeWithDirectExecutor(dispatcher, createRequest()));
        Assertions.assertEquals(0, dispatcher.sends.get());
    }

    // ---------------------------------------------------------------- wire request shape

    @Test
    public void testWireRequestCarriesDispatchIdentityAndPayload() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexSchemaContract contract = new LanceIndexSchemaContract(Collections.emptyList());
        long deadline = System.currentTimeMillis() + 123_456L;
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", deadline)
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setProperties(Collections.singletonMap("metric", "l2"))
                .setIfNotExists(true)
                .setSchemaContract(contract)
                .build();
        executeWithDirectExecutor(dispatcher, request);
        TLanceIndexMutationRequest wire = dispatcher.requests.get(0);
        Assertions.assertFalse(wire.getInvocationId().isEmpty());
        Assertions.assertEquals(BE_PROCESS_EPOCH, wire.getBeProcessEpoch());
        Assertions.assertEquals(deadline, wire.getDeadlineMs());
        Assertions.assertEquals(TLanceIndexMutationType.CREATE, wire.getMutationType());
        Assertions.assertEquals("idx", wire.getIndexName());
        Assertions.assertEquals("v", wire.getColumnName());
        Assertions.assertEquals("IVF_PQ", wire.getIndexType());
        Assertions.assertEquals("{\"metric\":\"l2\"}", wire.getPropertiesJson());
        Assertions.assertTrue(wire.isIfNotExists());
        Assertions.assertFalse(wire.isIfExists());
        Assertions.assertEquals(DATASET_URI, wire.getDatasetUri());
        Assertions.assertEquals(DATASET_VERSION, wire.getAdmittedDatasetVersion());
        Assertions.assertTrue(wire.isSetSchemaContractJson());
        Assertions.assertTrue(wire.getSchemaContractJson().contains("\"scv\":1"));
        // Storage options are resolved from the catalog at send time and travel untranslated.
        Assertions.assertTrue(wire.isSetStorageOptions());
    }

    @Test
    public void testDropCarriesNoSchemaContract() throws Exception {
        FakeDispatcher dispatcher = new FakeDispatcher(request -> okResult(0));
        LanceIndexMutationExecutor.MutationRequest request = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.DROP, DATASET_URI,
                        DATASET_VERSION, "idx", DEFAULT_DEADLINE_MS)
                .setIfExists(true)
                .build();
        executeWithDirectExecutor(dispatcher, request);
        TLanceIndexMutationRequest wire = dispatcher.requests.get(0);
        Assertions.assertFalse(wire.isSetSchemaContractJson());
        Assertions.assertFalse(wire.isSetPropertiesJson());
        Assertions.assertTrue(wire.isIfExists());
        Assertions.assertEquals("", wire.getColumnName());
    }

    // ---------------------------------------------------------------- budget helper

    @Test
    public void testBudgetDeadlineIsCapped() {
        int originalCap = Config.lance_index_mutation_budget_cap_seconds;
        try {
            Config.lance_index_mutation_budget_cap_seconds = 5;
            long start = System.currentTimeMillis();
            long deadline = LanceIndexMutationExecutor.budgetDeadlineMs(start);
            Assertions.assertTrue(deadline - start >= 4_900L && deadline - start <= 5_100L,
                    "budget must be the config cap when no statement context exists");
        } finally {
            Config.lance_index_mutation_budget_cap_seconds = originalCap;
        }
    }

    // ---------------------------------------------------------------- helpers

    private LanceIndexMutationExecutor.MutationRequest createRequest() {
        return requestBuilder("idx").build();
    }

    private LanceIndexMutationExecutor.MutationRequest.Builder requestBuilder(String indexName) {
        return requestBuilder(indexName, DEFAULT_DEADLINE_MS);
    }

    private LanceIndexMutationExecutor.MutationRequest.Builder requestBuilder(String indexName,
            long deadlineOffsetMs) {
        return LanceIndexMutationExecutor.MutationRequest.newBuilder(catalog,
                TLanceIndexMutationType.CREATE, DATASET_URI, DATASET_VERSION, indexName,
                System.currentTimeMillis() + deadlineOffsetMs)
                .setColumnName("v")
                .setIndexType("IVF_PQ");
    }

    private LanceIndexMutationOutcome executeWithDirectExecutor(
            LanceIndexMutationDispatcher dispatcher,
            LanceIndexMutationExecutor.MutationRequest request) throws Exception {
        // The direct executor runs the entry phases on the caller thread, mirroring a statement
        // whose admission is immediate.
        return LanceIndexMutationExecutor.execute(request, dispatcher,
                MoreExecutors.newDirectExecutorService());
    }

    private static boolean awaitInFlightCount(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            if (LanceIndexMutationExecutor.inFlightCount() == expected) {
                return true;
            }
            Thread.sleep(10L);
        }
        return LanceIndexMutationExecutor.inFlightCount() == expected;
    }

    private static TLanceIndexMutationResult okResult(int lanceResultCode) {
        TLanceIndexMutationResult result = new TLanceIndexMutationResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        result.setLanceErrorCode(lanceResultCode);
        result.setMessage("worker diagnostic");
        return result;
    }
}
