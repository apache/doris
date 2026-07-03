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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Guards the Kerberos scan-planning seam (the FOURTH plugin-side UGI doAs locus, after DDL /
 * write-FileIO / temp()): {@code planScan}'s manifest-list + manifest reads must route through the
 * plugin-side {@code doAs}, not just the {@code loadTable} inside {@code resolveTable}.
 *
 * <p>WHY it matters: on a Kerberos hadoop catalog the plugin bundles hadoop child-first, so its HDFS
 * FileSystem reads the PLUGIN's UserGroupInformation copy — logged in only inside the plugin
 * authenticator's {@code doAs}. {@code SnapshotScan.planFiles()} reads {@code snap-*.avro} through
 * {@code table.io()} on the planning thread (and iceberg's shared worker pool for multi-manifest
 * tables, which never inherits a caller-thread doAs) — CI proof: test_iceberg_hadoop_catalog_kerberos'
 * SELECT after INSERT failed SASL ("Client cannot authenticate via:[TOKEN, KERBEROS]") at exactly this
 * read once the INSERT-side loci were fixed. Red on "manifest reads escape doAs" = that regression.
 *
 * <p>No Mockito — real {@link InMemoryCatalog} tables and recording doubles, mirroring
 * {@link IcebergScanPlanProviderTest} / {@code TcclPinningConnectorContextTest}.
 */
public class IcebergScanPlanProviderKerberosScanIoTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Table oneFileTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(
                TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(DataFiles.builder(table.spec())
                        .withPath("s3://b/db/t1/f1.parquet")
                        .withFileSizeInBytes(1024)
                        .withRecordCount(10)
                        .withFormat(FileFormat.PARQUET)
                        .build())
                .commit();
        return table;
    }

    private static RecordingIcebergCatalogOps opsReturning(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return ops;
    }

    /** Counts plugin-side doAs invocations; runs the action inline (no real UGI/KDC involved). */
    private static final class RecordingAuthenticator implements HadoopAuthenticator {
        int doAsCount;

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("doAs is overridden; no real UGI in this test");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            doAsCount++;
            try {
                return action.run();
            } catch (IOException e) {
                throw e;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void kerberosPlanScanRoutesManifestReadsThroughPluginDoAs() {
        Table table = oneFileTable();
        RecordingAuthenticator auth = new RecordingAuthenticator();
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(
                new RecordingConnectorContext(), getClass().getClassLoader(), () -> auth);
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertEquals(1, ranges.size());
        // MUTATION: dropping wrapTableForScan from resolveTable leaves exactly ONE doAs (the loadTable wrap)
        // and planFiles' manifest-list/manifest reads escape the plugin doAs -> the CI SASL failure -> red.
        Assertions.assertTrue(auth.doAsCount > 1,
                "planFiles must read the manifest list/manifests through the authenticated FileIO "
                        + "(factory-time doAs); got only " + auth.doAsCount
                        + " doAs call(s), i.e. nothing beyond the resolveTable loadTable wrap");
    }

    @Test
    public void nonKerberosPlanScanKeepsDelegatePathAndNoWrap() {
        Table table = oneFileTable();
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        // Null plugin authenticator = non-Kerberos catalog: executeAuthenticated must delegate as-is and
        // the resolved table must NOT be wrapped (byte-preserved legacy behavior).
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(
                delegate, getClass().getClassLoader(), () -> null);
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList(), Optional.empty());

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(1, delegate.authCount,
                "non-Kerberos must keep exactly the one delegate executeAuthenticated (loadTable)");
        Assertions.assertSame(table, provider.wrapTableForScan(table),
                "non-Kerberos wrap must be an identity pass-through");
    }

    @Test
    public void wrapTableForScanWrapsIoFactoryCallsInPluginDoAs() {
        Table table = oneFileTable();
        RecordingAuthenticator auth = new RecordingAuthenticator();
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(
                new RecordingConnectorContext(), getClass().getClassLoader(), () -> auth);
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        Table wrapped = provider.wrapTableForScan(table);

        Assertions.assertNotSame(table, wrapped);
        Assertions.assertTrue(wrapped instanceof BaseTable, "wrap must preserve BaseTable-ness "
                + "(getFormatVersion and MetadataTableUtils cast to BaseTable)");
        int before = auth.doAsCount;
        wrapped.io().newInputFile(table.currentSnapshot().manifestListLocation());
        Assertions.assertEquals(before + 1, auth.doAsCount,
                "io() factory calls must run inside the plugin doAs — that is what captures the secured "
                        + "FileSystem for later newStream() on any thread (incl. iceberg's worker pool)");
    }

    @Test
    public void plainContextWrapIsIdentityPassThrough() {
        Table table = oneFileTable();
        // A non-TcclPinning context (offline tests / fe-core fakes) must never be wrapped.
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                Collections.emptyMap(), opsReturning(table), new RecordingConnectorContext());

        Assertions.assertSame(table, provider.wrapTableForScan(table));
    }

    @Test
    public void sysTablePlanningRunsInsideAuthScope() {
        Table table = oneFileTable();
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider =
                new IcebergScanPlanProvider(Collections.emptyMap(), opsReturning(table), context);

        List<ConnectorScanRange> ranges = provider.planScan(
                null,
                IcebergTableHandle.forSystemTable("db1", "t1", "snapshots", -1, null, -1),
                Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(ranges.isEmpty(), "one-snapshot table must plan $snapshots tasks");
        // MUTATION: dropping the planSystemTableScan thread-level wrap (whose ONE scope spans the base-table
        // load AND the metadata-table planFiles — resolveSysTable carries no wrap of its own) -> authCount 0
        // -> red: the $files-family manifest-list read on the planning thread escapes the Kerberos doAs.
        // (Object-level wrap is deliberately NOT used here: the planned FileScanTasks are Java-serialized
        // for the BE JNI reader.)
        Assertions.assertEquals(1, context.authCount,
                "system-table planning (base load + planFiles + task serialization) must sit inside "
                        + "exactly ONE executeAuthenticated scope");
    }
}
