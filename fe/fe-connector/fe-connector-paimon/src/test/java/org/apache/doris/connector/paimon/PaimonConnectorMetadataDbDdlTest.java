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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * T14 database-DDL tests for {@link PaimonConnectorMetadata#supportsCreateDatabase},
 * {@link #createDatabase} and the 4-arg {@link #dropDatabase}, pinning:
 * (1) the HMS-only-props gate runs as a pure local arg check BEFORE the authenticator,
 * (2) raw paimon checked exceptions are wrapped as {@link DorisConnectorException},
 * (3) D7=B: every remote call runs INSIDE
 *     {@link org.apache.doris.connector.spi.ConnectorContext#executeAuthenticated}, and
 * (4) the force-drop enumerate-loop + native cascade (legacy parity with
 *     {@code PaimonMetadataOps.performDropDb}).
 *
 * <p>All tests run offline against the recording seam fake (null real Catalog).
 */
public class PaimonConnectorMetadataDbDdlTest {

    /** Metadata with default (filesystem) flavor: catalogProperties has no paimon.catalog.type. */
    private static PaimonConnectorMetadata filesystemMetadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), ctx);
    }

    /** Metadata with HMS flavor: catalogProperties carries paimon.catalog.type=hms. */
    private static PaimonConnectorMetadata hmsMetadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(PaimonConnectorProperties.PAIMON_CATALOG_TYPE, PaimonConnectorProperties.HMS);
        return new PaimonConnectorMetadata(ops, catalogProps, ctx);
    }

    private static Map<String, String> dbProps() {
        Map<String, String> props = new HashMap<>();
        props.put("location", "/wh/db");
        return props;
    }

    // ==================== supportsCreateDatabase ====================

    @Test
    public void supportsCreateDatabaseIsTrue() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: supportsCreateDatabase()==true is the gate that makes
        // PluginDrivenExternalCatalog.createDb run the remote IF-NOT-EXISTS precheck AND route to
        // createDatabase; if it were false, CREATE DATABASE would fall through to "not supported".
        // MUTATION: returning false (the SPI default) makes this red and breaks the FE routing.
        Assertions.assertTrue(filesystemMetadata(ops, ctx).supportsCreateDatabase());
    }

    // ==================== createDatabase: HMS-only-props gate ====================

    @Test
    public void createDatabaseRejectsPropsForNonHmsFlavorBeforeAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY (legacy performCreateDb:103-109): only the HMS catalog type accepts CREATE DATABASE
        // properties; every other flavor (here: default filesystem) must reject non-empty props.
        // The gate is a pure local arg check, so it must run BEFORE executeAuthenticated and before
        // any seam call. MUTATION: if the gate were removed or placed AFTER executeAuthenticated,
        // authCount would be 1 and the seam log would contain createDatabase -> the two assertions
        // below (authCount==0, log empty) flip red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> filesystemMetadata(ops, ctx).createDatabase(null, "db1", dbProps()));
        Assertions.assertTrue(ex.getMessage().contains("filesystem"),
                "rejection message must name the offending catalog type");
        Assertions.assertEquals(0, ctx.authCount,
                "local arg-check rejection must abort BEFORE entering the authenticator");
        Assertions.assertTrue(ops.log.isEmpty(),
                "local arg-check rejection must never reach the remote catalog seam");
    }

    @Test
    public void createDatabaseAllowsPropsForHmsFlavor() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Map<String, String> props = dbProps();

        hmsMetadata(ops, ctx).createDatabase(null, "db1", props);

        // WHY: for the HMS flavor the gate must NOT fire; the props must be forwarded verbatim to
        // the seam, under exactly one authenticator scope. MUTATION: if the gate fired for HMS, this
        // would throw; if the props were dropped, lastCreatedDbProps would differ.
        Assertions.assertEquals(Collections.singletonList("createDatabase:db1"), ops.log);
        Assertions.assertEquals("db1", ops.lastCreatedDb);
        Assertions.assertEquals(props, ops.lastCreatedDbProps);
        Assertions.assertFalse(ops.lastCreateDbIgnoreIfExists,
                "ignoreIfExists must be false: FE already did the IF NOT EXISTS short-circuit");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void createDatabaseAllowsEmptyPropsForFilesystemFlavor() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        filesystemMetadata(ops, ctx).createDatabase(null, "db1", Collections.emptyMap());

        // WHY: the gate only rejects NON-EMPTY props on non-HMS flavors; an empty-props CREATE
        // DATABASE on filesystem is the common case and must succeed and reach the seam.
        // MUTATION: a gate that also rejected empty props (e.g. dropped the !isEmpty() guard) would
        // throw here.
        Assertions.assertEquals(Collections.singletonList("createDatabase:db1"), ops.log);
        Assertions.assertEquals("db1", ops.lastCreatedDb);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== createDatabase: exception wrap + authenticator ====================

    @Test
    public void createDatabaseWrapsDatabaseAlreadyExistAsDorisConnectorException() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseAlreadyExist = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: fe-core only understands DorisConnectorException; a raw paimon
        // DatabaseAlreadyExistException leaking out would bypass the engine's DDL error handling.
        // MUTATION: removing the try/catch wrap lets the raw paimon exception escape -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> filesystemMetadata(ops, ctx).createDatabase(null, "db1", Collections.emptyMap()));
        Assertions.assertTrue(ex.getMessage().contains("db1"),
                "wrapped message must name the database");
    }

    @Test
    public void createDatabaseRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        // WHY (D7=B legacy parity): the remote create must run inside executeAuthenticated so the
        // FE-injected auth context applies. When auth fails, the seam call must NOT have run.
        // This uses the NON-gated path (filesystem + empty props) so the gate cannot mask the test.
        // MUTATION: if createDatabase called catalogOps.createDatabase directly instead of inside
        // context.executeAuthenticated, the log would contain "createDatabase:db1" despite the auth
        // failure -> the log-empty assertion below would fail.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> filesystemMetadata(ops, ctx).createDatabase(null, "db1", Collections.emptyMap()));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the seam createDatabase call runs");
        Assertions.assertEquals(1, ctx.authCount,
                "createDatabase must enter executeAuthenticated exactly once");
    }

    // ==================== dropDatabase ====================

    @Test
    public void dropDatabaseForceEnumeratesAndCascades() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        filesystemMetadata(ops, ctx).dropDatabase(null, "db1", false, true);

        // WHY (legacy performDropDb:147-163): force-drop must FIRST enumerate the db's tables and
        // drop each (belt), THEN drop the db passing force as paimon's native cascade (suspenders).
        // The whole op runs under ONE authenticator scope. The exact log order pins both the
        // enumerate-then-drop sequencing and the native cascade=true forwarding.
        // MUTATION: if the enumerate-loop were skipped, the dropTable entries vanish; if force
        // weren't forwarded as cascade, the last entry would read cascade=false; if each remote call
        // got its own authenticator, authCount would be > 1.
        Assertions.assertEquals(
                Arrays.asList("listTables:db1", "dropTable:db1.t1", "dropTable:db1.t2",
                        "dropDatabase:db1,cascade=true"),
                ops.log);
        Assertions.assertEquals("db1", ops.lastDroppedDb);
        Assertions.assertTrue(ops.lastDropCascade, "force must be forwarded as native cascade=true");
        Assertions.assertTrue(ops.lastDropTableIgnoreIfNotExists,
                "cascaded per-table drops must be idempotent (ignoreIfNotExists=true)");
        Assertions.assertEquals(1, ctx.authCount,
                "the whole force-drop op runs under exactly one authenticator scope");
    }

    @Test
    public void dropDatabaseForceOnEmptyDbStillCascades() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Collections.emptyList();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        filesystemMetadata(ops, ctx).dropDatabase(null, "db1", false, true);

        // WHY: the FORCE enumerate-loop must no-op safely when the database has no tables and
        //   still perform the native cascade drop -- an empty db is the common force-drop case.
        // MUTATION: if the loop skipped the trailing dropDatabase when tables is empty, or
        //   emitted a spurious dropTable, the exact-log assertion would fail.
        Assertions.assertEquals(
                Arrays.asList("listTables:db1", "dropDatabase:db1,cascade=true"), ops.log);
        Assertions.assertTrue(ops.lastDropCascade, "force must be forwarded as native cascade=true");
        Assertions.assertEquals(1, ctx.authCount,
                "the whole force-drop op runs under exactly one authenticator scope");
    }

    @Test
    public void dropDatabaseNonForceSkipsEnumerateLoop() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        filesystemMetadata(ops, ctx).dropDatabase(null, "db1", false, false);

        // WHY: without force, the enumerate-loop must NOT run (no listTables / dropTable) and the
        // db drop must pass cascade=false, so paimon throws DatabaseNotEmptyException on a non-empty
        // db rather than silently deleting tables. MUTATION: if force weren't honored (loop always
        // runs), the log would contain listTables/dropTable; if cascade weren't false, the last
        // entry would read cascade=true.
        Assertions.assertEquals(
                Collections.singletonList("dropDatabase:db1,cascade=false"), ops.log);
        Assertions.assertFalse(ops.lastDropCascade);
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void dropDatabaseWrapsDatabaseNotEmptyAsDorisConnectorException() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseNotEmpty = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: a non-force DROP DATABASE on a non-empty db surfaces paimon's
        // DatabaseNotEmptyException, which must be wrapped so fe-core's DDL error handling applies.
        // MUTATION: removing the try/catch wrap lets the raw paimon exception escape -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> filesystemMetadata(ops, ctx).dropDatabase(null, "db1", false, false));
        Assertions.assertTrue(ex.getMessage().contains("db1"),
                "wrapped message must name the database");
    }

    @Test
    public void dropDatabaseRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        // WHY (D7=B legacy parity): the remote drop must run inside executeAuthenticated. When auth
        // fails, NO seam call (neither enumerate nor db drop) must run.
        // MUTATION: if dropDatabase called the seam directly instead of inside
        // context.executeAuthenticated, the log would be non-empty despite the auth failure.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> filesystemMetadata(ops, ctx).dropDatabase(null, "db1", false, true));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE any seam drop call runs");
        Assertions.assertEquals(1, ctx.authCount);
    }
}
