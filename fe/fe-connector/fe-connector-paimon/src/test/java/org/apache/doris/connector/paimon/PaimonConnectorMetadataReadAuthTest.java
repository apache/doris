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
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.paimon.partition.Partition;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * M-11 (rereview2 #6) read-path Kerberos doAs tests: every remote metadata READ RPC must run INSIDE
 * {@link org.apache.doris.connector.spi.ConnectorContext#executeAuthenticated}, for full legacy
 * parity (D-052) with legacy {@code PaimonMetadataOps}/{@code PaimonExternalCatalog} which wrapped
 * every read. Mirrors the DDL-path tests in {@link PaimonConnectorMetadataDdlTest}.
 *
 * <p>Uses {@link RecordingConnectorContext#failAuth} (throws WITHOUT running the task) to prove each
 * seam call sits INSIDE the authenticator: if a read called the {@link RecordingPaimonCatalogOps}
 * seam directly, the recording fake would log the call despite the auth failure. The happy-path
 * companions assert {@link RecordingConnectorContext#authCount} so dropping a wrap also goes red.
 *
 * <p>All offline against the recording seam (null real catalog).
 */
public class PaimonConnectorMetadataReadAuthTest {

    private static PaimonConnectorMetadata metadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), ctx);
    }

    private static PaimonTableHandle baseHandle() {
        return new PaimonTableHandle("db1", "t1", Collections.emptyList(), Collections.emptyList());
    }

    private static FakePaimonTable singleColTable(String col) {
        return new FakePaimonTable("t1",
                RowType.builder().field(col, DataTypes.STRING()).build(),
                Collections.emptyList(), Collections.emptyList());
    }

    // ==================== listDatabaseNames ====================

    @Test
    public void listDatabaseNamesRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        // listDatabaseNames swallows failures and returns empty; the proof is that the seam NEVER ran
        // (log empty) yet the authenticator was entered. MUTATION: an un-wrapped direct
        // catalogOps.listDatabases() would log "listDatabases" despite the auth failure -> red.
        Assertions.assertTrue(metadata(ops, ctx).listDatabaseNames(null).isEmpty());
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the listDatabases seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void listDatabaseNamesEntersAuthenticatorOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.databases = Arrays.asList("db1", "db2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Assertions.assertEquals(Arrays.asList("db1", "db2"), metadata(ops, ctx).listDatabaseNames(null));
        Assertions.assertEquals(Collections.singletonList("listDatabases"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== databaseExists ====================

    @Test
    public void databaseExistsRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        // databaseExists rethrows the auth failure as DorisConnectorException; the seam never ran.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).databaseExists(null, "db1"));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the getDatabase seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void databaseExistsTrueAndFalseEnterAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Assertions.assertTrue(metadata(ops, ctx).databaseExists(null, "db1"));
        Assertions.assertEquals(Collections.singletonList("getDatabase:db1"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);

        // DatabaseNotExistException is caught INSIDE the lambda (Kerberos UGI.doAs would otherwise
        // wrap the checked exception, defeating an outer catch) -> returns false, no rethrow.
        RecordingPaimonCatalogOps ops2 = new RecordingPaimonCatalogOps();
        ops2.throwDatabaseNotExist = true;
        RecordingConnectorContext ctx2 = new RecordingConnectorContext();
        Assertions.assertFalse(metadata(ops2, ctx2).databaseExists(null, "db1"));
        Assertions.assertEquals(1, ctx2.authCount);
    }

    // ==================== listTableNames ====================

    @Test
    public void listTableNamesRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Assertions.assertTrue(metadata(ops, ctx).listTableNames(null, "db1").isEmpty());
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the listTables seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void listTableNamesEntersAuthenticatorOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Assertions.assertEquals(Arrays.asList("t1", "t2"), metadata(ops, ctx).listTableNames(null, "db1"));
        Assertions.assertEquals(Collections.singletonList("listTables:db1"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== getTableHandle ====================

    @Test
    public void getTableHandleRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        Optional<ConnectorTableHandle> result = metadata(ops, ctx).getTableHandle(null, "db1", "t1");
        Assertions.assertFalse(result.isPresent());
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the getTable seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void getTableHandleEntersAuthenticatorOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = singleColTable("id");
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Assertions.assertTrue(metadata(ops, ctx).getTableHandle(null, "db1", "t1").isPresent());
        Assertions.assertEquals(Collections.singletonList("getTable:db1.t1"), ops.log);
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== getSysTableHandle ====================

    @Test
    public void getSysTableHandleRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        // getSysTableHandle rethrows the auth failure as RuntimeException; the sys getTable never ran.
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata(ops, ctx).getSysTableHandle(null, baseHandle(), "snapshots"));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the sys getTable seam runs");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void getSysTableHandleEntersAuthenticatorOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.sysTable = new FakePaimonTable("t1$snapshots",
                RowType.builder().field("snapshot_id", DataTypes.BIGINT()).build(),
                Collections.emptyList(), Collections.emptyList());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        Assertions.assertTrue(metadata(ops, ctx).getSysTableHandle(null, baseHandle(), "snapshots").isPresent());
        Assertions.assertEquals(1, ctx.authCount);
    }

    // ==================== listPartitions (resolveTable + listPartitions both wrapped) ====================

    @Test
    public void listPartitionNamesEntersAuthenticatorForBothResolveAndListPartitions() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable("t1",
                RowType.builder().field("region", DataTypes.STRING()).build(),
                Collections.singletonList("region"), Collections.emptyList());
        ops.table = table;
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("region", "cn");
        ops.partitions = Collections.singletonList(
                new Partition(spec, 1L, 1L, /*fileCount*/ 1, 1L, /*done*/ true));
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.singletonList("region"), Collections.emptyList());
        handle.setPaimonTable(table);

        List<String> names = metadata(ops, ctx).listPartitionNames(null, handle);

        Assertions.assertEquals(Collections.singletonList("region=cn"), names);
        // WHY: BOTH the table resolution (resolveTable) AND the listPartitions RPC must each run inside
        // executeAuthenticated (D-052). The handle carries a transient table so resolveTable issues no
        // getTable RPC, but it STILL enters the authenticator; listPartitions then enters it again.
        // MUTATION: dropping the listPartitions wrap leaves authCount==1 (only resolveTable) -> red;
        // dropping the resolveTable wrap leaves authCount==1 (only listPartitions) -> red.
        Assertions.assertEquals(2, ctx.authCount);
        Assertions.assertEquals(Collections.singletonList("listPartitions:db1.t1"), ops.log);
    }

    @Test
    public void listPartitionNamesAbortsInsideAuthenticatorOnAuthFailure() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = singleColTable("region");
        ops.table = table;
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.singletonList("region"), Collections.emptyList());
        handle.setPaimonTable(table);

        // The FIRST wrapped op (resolveTable) aborts under failAuth, so collectPartitions throws and
        // neither getTable nor listPartitions ever runs.
        Assertions.assertThrows(RuntimeException.class,
                () -> metadata(ops, ctx).listPartitionNames(null, handle));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE any partition-path seam runs");
    }
}
