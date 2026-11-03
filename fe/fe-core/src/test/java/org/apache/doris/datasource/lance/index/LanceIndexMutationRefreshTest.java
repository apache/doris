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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceIndexMutationOutcome;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The refresh obligation table of the synchronous Lance index mutation path (design v6
 * section 6.4, PR1 subset), one assertion per row: a trusted commit invalidates the local
 * table caches and a failed invalidation downgrades to committed-refresh-incomplete (never a
 * build failure); a no-op invalidates too and a residual failure is the typed refresh error
 * of the confirmed family; a commit-conflict confirmed failure gets a best-effort refresh
 * whose failure only attaches a diagnostic; every other class owes nothing. Plus the
 * production refresher itself: one local ExternalMetaCacheMgr call, bounded retry, honest
 * failure reason.
 */
public class LanceIndexMutationRefreshTest {

    private static final long CATALOG_ID = 10L;
    private static final String DATASET_URI = "s3://bucket/dataset";
    private static final long DATASET_VERSION = 7L;
    private static final long BE_ID = 1L;

    private LanceExternalCatalog catalog;
    private LanceExternalTable table;
    private SystemInfoService systemInfo;
    private BooleanSupplier originalMasterCheck;
    private Supplier<SystemInfoService> originalBackendInventory;

    private static final class FakeRefresher implements LanceIndexMutationRefresher {
        final AtomicInteger calls = new AtomicInteger();
        private final String result;

        FakeRefresher(String result) {
            this.result = result;
        }

        @Override
        public String invalidateTableMetadata() {
            calls.incrementAndGet();
            return result;
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
        table = Mockito.mock(LanceExternalTable.class);

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getBePort()).thenReturn(9020);
        Mockito.when(backend.getProcessEpoch()).thenReturn(42L);
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

    // ------------------------------------------------------- obligation table, executor rows

    @Test
    public void testConfirmedCommitRefreshesLocalTableMetadata() throws Exception {
        FakeRefresher refresher = new FakeRefresher(null);
        LanceIndexMutationOutcome outcome = execute(dispatcherReturning(0), refresher);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS, outcome.getKind());
        Assertions.assertEquals(1, refresher.calls.get());
    }

    @Test
    public void testCommitWithFailedRefreshIsReportedCommittedRefreshIncomplete() throws Exception {
        FakeRefresher refresher = new FakeRefresher("local table metadata invalidation failed"
                + " after 3 attempts: IllegalStateException");
        LanceIndexMutationOutcome outcome = execute(dispatcherReturning(0), refresher);
        // The commit is a fact: never a build failure, never denied, its own class.
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.COMMITTED_REFRESH_INCOMPLETE,
                outcome.getKind());
        AnalysisException exception = outcome.toUserException();
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_COMMITTED_REFRESH_INCOMPLETE,
                exception.getMysqlErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("committed"),
                "the client-facing text must state the commit: " + exception.getMessage());
        Assertions.assertTrue(outcome.getMessage().contains("the commit stands"));
    }

    @Test
    public void testNoOpRefreshesLocalTableMetadata() throws Exception {
        FakeRefresher refresher = new FakeRefresher(null);
        RecordingDispatcher dispatcher = dispatcherReturning(0);
        LanceIndexMutationOutcome outcome = execute(
                dispatcher, refresher, LanceIndexMutationExecutor.PreflightResult.NO_OP);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.SUCCESS, outcome.getKind());
        Assertions.assertEquals(0, dispatcher.sends.get());
        Assertions.assertEquals(1, refresher.calls.get());
    }

    @Test
    public void testNoOpWithFailedRefreshReportsTypedRefreshError() {
        FakeRefresher refresher = new FakeRefresher("local table metadata invalidation failed"
                + " after 3 attempts: IllegalStateException");
        LanceIndexMutationExecutor.MutationRequest request = request(refresher,
                LanceIndexMutationExecutor.PreflightResult.NO_OP);
        RecordingDispatcher dispatcher = new RecordingDispatcher(0);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> LanceIndexMutationExecutor.execute(request, dispatcher,
                        MoreExecutors.newDirectExecutorService()));
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_REFRESH_FAILED,
                exception.getMysqlErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("no-op"),
                "the client-facing text must state the no-op: " + exception.getMessage());
        // Nothing was mutated and nothing was sent.
        Assertions.assertEquals(1, refresher.calls.get());
        Assertions.assertEquals(0, dispatcher.sends.get());
    }

    @Test
    public void testCommitConflictRefreshesExternallyAdvancedMetadata() throws Exception {
        FakeRefresher refresher = new FakeRefresher(null);
        LanceIndexMutationOutcome outcome = execute(
                dispatcherReturning(LanceIndexMutationExecutor.LANCE_ERR_COMMIT_CONFLICT), refresher);
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertEquals(LanceIndexMutationExecutor.LANCE_ERR_COMMIT_CONFLICT,
                outcome.getLanceResultCode());
        Assertions.assertEquals(1, refresher.calls.get());
    }

    @Test
    public void testCommitConflictWithFailedRefreshKeepsClassification() throws Exception {
        FakeRefresher refresher = new FakeRefresher("local table metadata invalidation failed"
                + " after 3 attempts: IllegalStateException");
        LanceIndexMutationOutcome outcome = execute(
                dispatcherReturning(LanceIndexMutationExecutor.LANCE_ERR_COMMIT_CONFLICT), refresher);
        // A best-effort refresh failure attaches a diagnostic and never changes the class.
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE, outcome.getKind());
        Assertions.assertEquals(ErrorCode.ERR_LANCE_INDEX_MUTATION_REJECTED,
                outcome.toUserException().getMysqlErrorCode());
        Assertions.assertTrue(outcome.getMessage().contains("best-effort"));
        Assertions.assertEquals(1, refresher.calls.get());
    }

    @Test
    public void testCleanRejectionAndIndeterminateOweNoRefresh() throws Exception {
        // A pre-execution backend rejection: nothing committed anywhere, nothing to refresh.
        FakeRefresher refresher = new FakeRefresher(null);
        TStatus status = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);
        status.setErrorMsgs(Collections.singletonList("lance index worker is not available"));
        RecordingDispatcher rejected = new RecordingDispatcher(
                result -> new TLanceIndexMutationResult().setStatus(status));
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.CONFIRMED_FAILURE,
                execute(rejected, refresher).getKind());
        Assertions.assertEquals(0, refresher.calls.get());

        // An indeterminate result changes nothing the FE knows; it owes no refresh either.
        RecordingDispatcher lost = new RecordingDispatcher(result -> {
            throw new TTransportException(TTransportException.TIMED_OUT, "timeout");
        });
        Assertions.assertEquals(LanceIndexMutationOutcome.Kind.INDETERMINATE,
                execute(lost, refresher).getKind());
        Assertions.assertEquals(0, refresher.calls.get());
    }

    @Test
    public void testSettingTheTableDerivesTheProductionRefresher() {
        LanceIndexMutationExecutor.MutationRequest withTable = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", System.currentTimeMillis() + 60_000L)
                .setTable(table)
                .build();
        // The statement wiring sets the table and must get the production refresher without
        // saying so; an explicit refresher still wins; neither yields the no-op default.
        Assertions.assertInstanceOf(LanceIndexMutationRefresher.LocalTableInvalidation.class,
                withTable.getRefresher());
        FakeRefresher explicit = new FakeRefresher(null);
        LanceIndexMutationExecutor.MutationRequest overridden = LanceIndexMutationExecutor
                .MutationRequest.newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI,
                        DATASET_VERSION, "idx", System.currentTimeMillis() + 60_000L)
                .setTable(table)
                .setRefresher(explicit)
                .build();
        Assertions.assertSame(explicit, overridden.getRefresher());
    }

    // ------------------------------------------------------- production refresher

    @Test
    public void testProductionInvalidationCallsInvalidateTableCacheOnce() throws Exception {
        try (MockedEnvFixture fixture = MockedEnvFixture.withManager()) {
            LanceIndexMutationRefresher refresher = LanceIndexMutationRefresher.forTable(fixture.table);
            Assertions.assertNull(refresher.invalidateTableMetadata());
            Mockito.verify(fixture.manager, Mockito.times(1))
                    .invalidateTableCache(fixture.table);
        }
    }

    @Test
    public void testProductionInvalidationRetriesBoundedAndReportsHonestReason() throws Exception {
        try (MockedEnvFixture fixture = MockedEnvFixture.withManager()) {
            Mockito.doThrow(new RuntimeException("cache manager exploded"))
                    .when(fixture.manager).invalidateTableCache(fixture.table);
            LanceIndexMutationRefresher refresher = LanceIndexMutationRefresher.forTable(fixture.table);
            String reason = refresher.invalidateTableMetadata();
            Assertions.assertNotNull(reason);
            Assertions.assertTrue(reason.contains(
                    LanceIndexMutationRefresher.LocalTableInvalidation.MAX_ATTEMPTS + " attempts"),
                    reason);
            Mockito.verify(fixture.manager, Mockito.times(
                    LanceIndexMutationRefresher.LocalTableInvalidation.MAX_ATTEMPTS))
                    .invalidateTableCache(fixture.table);
        }
    }

    @Test
    public void testProductionInvalidationSucceedsOnRetry() throws Exception {
        try (MockedEnvFixture fixture = MockedEnvFixture.withManager()) {
            Mockito.doThrow(new RuntimeException("transient"))
                    .doThrow(new RuntimeException("transient"))
                    .doNothing()
                    .when(fixture.manager).invalidateTableCache(fixture.table);
            Assertions.assertNull(
                    LanceIndexMutationRefresher.forTable(fixture.table).invalidateTableMetadata());
            Mockito.verify(fixture.manager, Mockito.times(3)).invalidateTableCache(fixture.table);
        }
    }

    @Test
    public void testProductionInvalidationReportsMissingManager() throws Exception {
        try (MockedEnvFixture fixture = MockedEnvFixture.withoutManager()) {
            String reason = LanceIndexMutationRefresher.forTable(fixture.table)
                    .invalidateTableMetadata();
            Assertions.assertNotNull(reason);
            Assertions.assertTrue(reason.contains("attempts"), reason);
        }
    }

    // ------------------------------------------------------- helpers

    private static final class RecordingDispatcher implements LanceIndexMutationDispatcher {
        private final Answer answer;
        final AtomicInteger sends = new AtomicInteger();

        private interface Answer {
            TLanceIndexMutationResult answer(TLanceIndexMutationRequest request) throws Exception;
        }

        RecordingDispatcher(int lanceResultCode) {
            this(result -> {
                TLanceIndexMutationResult answer = new TLanceIndexMutationResult();
                answer.setStatus(new TStatus(TStatusCode.OK));
                answer.setLanceErrorCode(lanceResultCode);
                return answer;
            });
        }

        RecordingDispatcher(Answer answer) {
            this.answer = answer;
        }

        @Override
        public TLanceIndexMutationResult dispatch(TLanceIndexMutationRequest request, Backend backend,
                long timeoutMillis) throws Exception {
            sends.incrementAndGet();
            return answer.answer(request);
        }
    }

    private RecordingDispatcher dispatcherReturning(int lanceResultCode) {
        return new RecordingDispatcher(lanceResultCode);
    }

    private LanceIndexMutationOutcome execute(RecordingDispatcher dispatcher,
            LanceIndexMutationRefresher refresher) throws Exception {
        return execute(dispatcher, refresher, LanceIndexMutationExecutor.PreflightResult.PROCEED);
    }

    private LanceIndexMutationOutcome execute(RecordingDispatcher dispatcher,
            LanceIndexMutationRefresher refresher,
            LanceIndexMutationExecutor.PreflightResult preflight) throws Exception {
        return LanceIndexMutationExecutor.execute(request(refresher, preflight), dispatcher,
                MoreExecutors.newDirectExecutorService());
    }

    private LanceIndexMutationExecutor.MutationRequest request(LanceIndexMutationRefresher refresher,
            LanceIndexMutationExecutor.PreflightResult preflight) {
        return LanceIndexMutationExecutor.MutationRequest
                .newBuilder(catalog, TLanceIndexMutationType.CREATE, DATASET_URI, DATASET_VERSION,
                        "idx", System.currentTimeMillis() + 60_000L)
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setFinalPreflight(() -> preflight)
                .setRefresher(refresher)
                .build();
    }

    /**
     * Mocks {@link Env#getCurrentEnv()} for the production refresher on the test thread: with
     * an {@link ExternalMetaCacheMgr} (successful and retry paths) or without one (honest
     * failure reason).
     */
    private static final class MockedEnvFixture implements AutoCloseable {
        private final org.mockito.MockedStatic<Env> mockedEnv;
        private final Env env;
        private final ExternalMetaCacheMgr manager;
        private final LanceExternalTable table;

        private MockedEnvFixture(ExternalMetaCacheMgr manager) {
            this.mockedEnv = Mockito.mockStatic(Env.class);
            this.env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            this.manager = manager;
            if (manager != null) {
                Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
            }
            this.table = Mockito.mock(LanceExternalTable.class);
        }

        static MockedEnvFixture withManager() {
            return new MockedEnvFixture(Mockito.mock(ExternalMetaCacheMgr.class));
        }

        static MockedEnvFixture withoutManager() {
            return new MockedEnvFixture(null);
        }

        @Override
        public void close() {
            mockedEnv.close();
        }
    }
}
