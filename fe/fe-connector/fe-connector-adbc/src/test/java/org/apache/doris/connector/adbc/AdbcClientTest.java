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

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

/**
 * Exercises {@link AdbcClient} against the real JNI bridge and the real SQLite driver from thirdparty.
 * Skips (loudly) when those libraries are absent -- see {@link AdbcNativeTestSupport}.
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
}
