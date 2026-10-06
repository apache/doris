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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises {@link AdbcClient} against the real JNI bridge and the real SQLite driver from thirdparty.
 * Skips (loudly) when those libraries are absent -- see {@link AdbcNativeTestSupport}. The tests of when the
 * driver is released stand a {@link RecordingClient} in for it instead, so they run everywhere.
 */
class AdbcClientTest {

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    @Test
    void opensAConnectionThroughTheJniBridge(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("probe.db"))) {
            String catalog = client.withConnection(connection -> connection.getCurrentCatalog());
            // SQLite's single catalog. Asserting the value (not just "no exception") is what proves the call
            // reached the driver rather than stopping somewhere in the bridge.
            Assertions.assertEquals("main", catalog);
        }
    }

    @Test
    void reopensAfterTheFirstUseAndReleasesArrowMemoryOnClose(@TempDir Path tempDir) {
        AdbcClient client = sqliteClient(tempDir.resolve("probe.db"));
        try {
            client.withConnection(connection -> connection.getCurrentCatalog());
            client.withConnection(connection -> connection.getCurrentCatalog());
        } finally {
            // Closing must not throw; an allocator leak would surface here as an IllegalStateException from
            // Arrow ("Memory was leaked by query"), which is precisely the failure a per-catalog allocator
            // risks if a connection is left open.
            Assertions.assertDoesNotThrow(client::close);
        }
    }

    @Test
    void usingAClosedClientFailsLoud(@TempDir Path tempDir) {
        AdbcClient client = sqliteClient(tempDir.resolve("probe.db"));
        client.withConnection(connection -> connection.getCurrentCatalog());
        client.close();

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> client.withConnection(connection -> connection.getCurrentCatalog()));
        Assertions.assertTrue(e.getMessage().contains("closed"), e.getMessage());
    }

    @Test
    void closeLeavesACallThatIsStillUsingTheDriverAlone(@TempDir Path tempDir) throws Exception {
        AdbcClient client = sqliteClient(tempDir.resolve("probe.db"));
        CountDownLatch insideCall = new CountDownLatch(1);
        CountDownLatch mayContinue = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<String> call = caller.submit(() -> client.withConnection(connection -> {
                insideCall.countDown();
                Assertions.assertTrue(mayContinue.await(30, TimeUnit.SECONDS));
                // Back into the driver after the client was closed, as a statistics loader is when the catalog
                // it reads is dropped under it. Closing the database used to close this connection too; here
                // the JNI handle check turns that into an error, but a thread already inside the driver read
                // freed memory and took FE down with it (SIGSEGV in sqlite3FindTable).
                return connection.getCurrentCatalog();
            }));
            Assertions.assertTrue(insideCall.await(30, TimeUnit.SECONDS));
            closeWithoutWaitingForCalls(client);
            mayContinue.countDown();
            Assertions.assertEquals("main", call.get(30, TimeUnit.SECONDS));
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void theLastCallToLeaveAClosedClientReleasesTheDatabase(@TempDir Path tempDir) throws Exception {
        RecordingClient client = new RecordingClient(Files.createFile(tempDir.resolve("driver.so")));
        CountDownLatch insideCall = new CountDownLatch(1);
        CountDownLatch mayLeave = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<String> call = caller.submit(() -> client.withConnection(connection -> {
                insideCall.countDown();
                Assertions.assertTrue(mayLeave.await(30, TimeUnit.SECONDS));
                return "done";
            }));
            Assertions.assertTrue(insideCall.await(30, TimeUnit.SECONDS));

            closeWithoutWaitingForCalls(client);
            // Refused from now on, yet nothing is released while a call is still inside.
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> client.withConnection(connection -> "late"));
            Assertions.assertTrue(e.getMessage().contains("closed"), e.getMessage());
            Assertions.assertEquals(0, client.database.closes.get());

            mayLeave.countDown();
            Assertions.assertEquals("done", call.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(1, client.database.closes.get());
        } finally {
            caller.shutdownNow();
        }
        client.close();
        Assertions.assertEquals(1, client.database.closes.get());
    }

    @Test
    void releaseFailingAsTheLastCallLeavesIsNotThatCallsFailure(@TempDir Path tempDir) throws Exception {
        RecordingClient client = new RecordingClient(Files.createFile(tempDir.resolve("driver.so")),
                new RecordingDatabase(true));
        CountDownLatch insideCall = new CountDownLatch(1);
        CountDownLatch mayLeave = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<String> call = caller.submit(() -> client.withConnection(connection -> {
                insideCall.countDown();
                Assertions.assertTrue(mayLeave.await(30, TimeUnit.SECONDS));
                return "done";
            }));
            Assertions.assertTrue(insideCall.await(30, TimeUnit.SECONDS));
            closeWithoutWaitingForCalls(client);

            mayLeave.countDown();
            // The call did its work; the driver failing to close as it leaves is logged, not thrown at a caller that
            // did not close the catalog - a statistics load or a statement that happened to be the last one in.
            Assertions.assertEquals("done", call.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(1, client.database.closes.get());
        } finally {
            caller.shutdownNow();
        }
        // Released, if unsuccessfully: nothing is closed again.
        client.close();
        Assertions.assertEquals(1, client.database.closes.get());
    }

    @Test
    void closingAClientNoCallIsUsingReleasesTheDatabaseAtOnce(@TempDir Path tempDir) throws Exception {
        RecordingClient client = new RecordingClient(Files.createFile(tempDir.resolve("driver.so")));
        Assertions.assertEquals("done", client.withConnection(connection -> "done"));
        Assertions.assertEquals(0, client.database.closes.get());

        client.close();
        Assertions.assertEquals(1, client.database.closes.get());
    }

    /** Fails instead of hanging the run if close() ever starts waiting for the calls still in flight. */
    private static void closeWithoutWaitingForCalls(AdbcClient client) throws Exception {
        CompletableFuture.runAsync(client::close).get(30, TimeUnit.SECONDS);
    }

    @Test
    void missingDriverFileFailsAtFirstUseNotAtConstruction(@TempDir Path tempDir) {
        // An FE follower replaying the edit log constructs every catalog; if construction reached the
        // filesystem, one node missing a driver file would stop FE from starting instead of failing that
        // one catalog.
        AdbcClient client = new AdbcClient(tempDir.resolve("absent.so"), "absent.so",
                null, "file:" + tempDir.resolve("x.db"), null, null, Map.of());

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> client.withConnection(connection -> connection.getCurrentCatalog()));
        Assertions.assertTrue(e.getMessage().contains("EVERY BE"), e.getMessage());
    }

    @Test
    void badEntrypointIsReportedWithTheDriverDetail(@TempDir Path tempDir) {
        AdbcClient client = new AdbcClient(AdbcNativeTestSupport.sqliteDriver(),
                "libadbc_driver_sqlite.so", "NoSuchSymbolXyz",
                "file:" + tempDir.resolve("probe.db"), null, null, Map.of());
        try {
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> client.withConnection(connection -> connection.getCurrentCatalog()));
            // Proves driver_entrypoint actually reaches the driver manager, and that a driver-side failure
            // arrives with enough detail to act on.
            Assertions.assertTrue(e.getMessage().contains("NoSuchSymbolXyz"), e.getMessage());
        } finally {
            client.close();
        }
    }

    @Test
    void unhelpfulDriverMessagesAreNotForwardedAsTheWholeError() {
        // The SQLite driver answers NOT_IMPLEMENTED with the literal text "(unknown error)". Forwarding it
        // verbatim would produce an error that names neither the operation nor the cause, so the status has
        // to carry the meaning instead.
        AdbcException unhelpful = new AdbcException("(unknown error)", null,
                AdbcStatusCode.NOT_IMPLEMENTED, null, 0);
        DorisConnectorException translated = AdbcClient.translate(unhelpful, "getTableSchema failed");

        Assertions.assertTrue(translated.getMessage().contains("getTableSchema failed"),
                translated.getMessage());
        Assertions.assertTrue(translated.getMessage().contains("NOT_IMPLEMENTED"), translated.getMessage());
        Assertions.assertFalse(translated.getMessage().contains("(unknown error)"),
                translated.getMessage());
    }

    @Test
    void meaningfulDriverMessagesAreKept() {
        AdbcException helpful = new AdbcException("relation \"t\" does not exist", null,
                AdbcStatusCode.NOT_FOUND, "42P01", 7);
        DorisConnectorException translated = AdbcClient.translate(helpful, "listTableNames failed");

        String message = translated.getMessage();
        Assertions.assertTrue(message.contains("relation \"t\" does not exist"), message);
        Assertions.assertTrue(message.contains("42P01"), message);
        Assertions.assertTrue(message.contains("7"), message);
    }

    /** An {@link AdbcClient} whose database records how it is used instead of reaching a driver. */
    private static final class RecordingClient extends AdbcClient {

        private final RecordingDatabase database;

        private RecordingClient(Path driver) {
            this(driver, new RecordingDatabase(false));
        }

        private RecordingClient(Path driver, RecordingDatabase database) {
            super(driver, driver.toString(), null, "file:/unused.db", null, null, Map.of());
            this.database = database;
        }

        @Override
        AdbcDatabase openDatabase(BufferAllocator allocator, Map<String, Object> parameters) {
            return database;
        }
    }

    private static final class RecordingDatabase implements AdbcDatabase {

        private final AtomicInteger openConnections = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final boolean closeFails;

        private RecordingDatabase(boolean closeFails) {
            this.closeFails = closeFails;
        }

        @Override
        public AdbcConnection connect() {
            openConnections.incrementAndGet();
            return new AdbcConnection() {
                @Override
                public AdbcStatement createStatement() {
                    throw new AssertionError("these tests run no statement");
                }

                @Override
                public ArrowReader getInfo(int[] infoCodes) {
                    throw new AssertionError("these tests ask for no info");
                }

                @Override
                public void close() {
                    openConnections.decrementAndGet();
                }
            };
        }

        @Override
        public void close() throws AdbcException {
            // The JNI driver closes every connection still open on the database first, from this thread --
            // whichever thread is still using one.
            Assertions.assertEquals(0, openConnections.get(), "the database was released under an open connection");
            closes.incrementAndGet();
            if (closeFails) {
                throw new AdbcException("the driver failed to close the database", null, AdbcStatusCode.IO, null, 0);
            }
        }
    }
}
