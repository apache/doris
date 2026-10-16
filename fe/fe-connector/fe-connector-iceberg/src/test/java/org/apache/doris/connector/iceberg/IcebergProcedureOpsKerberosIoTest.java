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

import org.apache.doris.connector.spi.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.kerberos.HadoopAuthenticator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Guards the Kerberos I/O seam of {@code ALTER TABLE ... EXECUTE}: every metadata file a procedure opens, lists or
 * deletes must be reached inside the plugin-side {@code doAs}, including the files iceberg opens on its shared
 * worker pool.
 *
 * <p>WHY it matters: {@link IcebergProcedureOps} runs the procedure inside {@code executeAuthenticated}, whose
 * {@code doAs} covers only the calling thread. {@code expire_snapshots} commits the new metadata and then reads the
 * expired snapshots' manifest lists on the worker pool; through the raw table FileIO those reads reach secured HDFS
 * without Kerberos credentials (CI: test_iceberg_hadoop_catalog_kerberos failed "Failed to open input stream for
 * file: .../snap-*.avro" on expire_snapshots after rollback_to_snapshot). The authenticator here marks only the
 * thread that entered {@code doAs}, as a Kerberos {@code Subject} is invisible to pool threads, and the table's
 * FileIO records every call made outside that mark.
 *
 * <p>No Mockito: real {@code HadoopTables} tables in a temp directory and recording doubles, mirroring
 * {@link IcebergScanPlanProviderKerberosScanIoTest}.
 */
public class IcebergProcedureOpsKerberosIoTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "t1");

    @Test
    public void expireSnapshotsReadsExpiredManifestListsInsidePluginDoAs(@TempDir Path temp) throws Exception {
        // The CI sequence: the rollback leaves the second snapshot outside main's ancestry, so the expiry walks the
        // reachable files and reads every expired manifest list on the worker pool.
        Table table = createTable(temp);
        appendFile(table, temp.resolve("s1.parquet"));
        long firstSnapshot = table.currentSnapshot().snapshotId();
        appendFile(table, temp.resolve("s2.parquet"));
        table.manageSnapshots().rollbackTo(firstSnapshot).commit();
        appendFile(table, temp.resolve("s3.parquet"));
        ThreadMarkingAuthenticator auth = new ThreadMarkingAuthenticator();
        DoAsCheckingFileIO io = new DoAsCheckingFileIO(auth);

        kerberosProcedureOps(withFileIO(table, io), auth).execute(null, HANDLE,
                "expire_snapshots", ImmutableMap.of("retain_last", "1"), null, Collections.emptyList());

        table.refresh();
        Assertions.assertEquals(1, ImmutableList.copyOf(table.snapshots()).size());
        // Only the second snapshot's data file is left unreferenced, and the cleanup deleted it.
        Assertions.assertFalse(Files.exists(temp.resolve("s2.parquet")));
        Assertions.assertTrue(Files.exists(temp.resolve("s1.parquet")));
        Assertions.assertTrue(Files.exists(temp.resolve("s3.parquet")));
        Assertions.assertTrue(io.workerThreadCalls.get() > 0,
                "the cleanup must have opened files on iceberg's worker pool, or this test proves nothing");
        // MUTATION: handing the action the loaded table instead of wrapTableForProcedure(table) -> the worker-pool
        // manifest-list reads run outside the doAs -> red (the CI failure).
        Assertions.assertEquals(Collections.emptyList(), io.callsOutsideDoAs);
    }

    @Test
    public void cherrypickSnapshotWritesItsManifestInsidePluginDoAs(@TempDir Path temp) throws Exception {
        // Main moves on after the append is staged, so the cherry-pick cannot fast-forward: iceberg writes the
        // picked files into a new manifest on its worker pool.
        Table table = createTable(temp);
        appendFile(table, temp.resolve("a.parquet"));
        long mainSnapshot = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(dataFile(createOldFile(temp.resolve("staged.parquet")))).stageOnly().commit();
        table.refresh();
        long stagedSnapshot = ImmutableList.copyOf(table.snapshots()).stream().mapToLong(Snapshot::snapshotId)
                .filter(id -> id != mainSnapshot).findFirst().getAsLong();
        appendFile(table, temp.resolve("b.parquet"));
        ThreadMarkingAuthenticator auth = new ThreadMarkingAuthenticator();
        DoAsCheckingFileIO io = new DoAsCheckingFileIO(auth);

        kerberosProcedureOps(withFileIO(table, io), auth).execute(null, HANDLE, "cherrypick_snapshot",
                ImmutableMap.of("snapshot_id", String.valueOf(stagedSnapshot)), null, Collections.emptyList());

        table.refresh();
        Assertions.assertEquals("3", table.currentSnapshot().summary().get("total-data-files"));
        Assertions.assertTrue(io.workerThreadCalls.get() > 0,
                "the cherry-pick must have written its manifest on iceberg's worker pool, or this test proves nothing");
        // MUTATION: handing the action the loaded table instead of wrapTableForProcedure(table) -> the worker-pool
        // manifest write runs outside the doAs -> red.
        Assertions.assertEquals(Collections.emptyList(), io.callsOutsideDoAs);
    }

    @Test
    public void removeOrphanFilesListsAndDeletesInsidePluginDoAs(@TempDir Path temp) throws Exception {
        Table table = createTable(temp);
        appendFile(table, temp.resolve("table/data/live.parquet"));
        Path orphan = createOldFile(temp.resolve("table/data/orphan.parquet"));
        ThreadMarkingAuthenticator auth = new ThreadMarkingAuthenticator();
        DoAsCheckingFileIO io = new DoAsCheckingFileIO(auth);

        // MUTATION: wrapping with a FileIO that drops the delegate's prefix listing -> "remove_orphan_files
        // requires FileIO prefix listing support" -> red.
        ConnectorProcedureResult result = kerberosProcedureOps(withFileIO(table, io), auth).execute(null, HANDLE,
                "remove_orphan_files", ImmutableMap.of(
                        "older_than", String.valueOf(System.currentTimeMillis() - Duration.ofHours(24).toMillis()),
                        "dry_run", "false"),
                null, Collections.emptyList());

        Assertions.assertEquals(ImmutableList.of("1", "1"), result.getRows().get(0));
        Assertions.assertFalse(Files.exists(orphan));
        Assertions.assertTrue(Files.exists(temp.resolve("table/data/live.parquet")));
        Assertions.assertTrue(io.listPrefixCalls.get() > 0, "the orphan scan must list through the table FileIO");
        Assertions.assertEquals(Collections.emptyList(), io.callsOutsideDoAs);
    }

    @Test
    public void rewritePlanningReadsManifestsInsidePluginDoAs(@TempDir Path temp) throws Exception {
        // Two fast appends leave two data manifests, so iceberg plans the scan on its worker pool.
        Table table = createTable(temp);
        appendFile(table, temp.resolve("a.parquet"));
        appendFile(table, temp.resolve("b.parquet"));
        ThreadMarkingAuthenticator auth = new ThreadMarkingAuthenticator();
        DoAsCheckingFileIO io = new DoAsCheckingFileIO(auth);

        List<ConnectorRewriteGroup> groups = kerberosProcedureOps(withFileIO(table, io), auth).planRewrite(null, HANDLE,
                "rewrite_data_files", ImmutableMap.of("rewrite-all", "true"), null, Collections.emptyList());

        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals(2, groups.get(0).getDataFileCount());
        Assertions.assertTrue(io.workerThreadCalls.get() > 0,
                "planning must have read manifests on iceberg's worker pool, or this test proves nothing");
        // MUTATION: planning the loaded table instead of wrapTableForProcedure(table) -> the worker-pool manifest
        // reads run outside the doAs -> red.
        Assertions.assertEquals(Collections.emptyList(), io.callsOutsideDoAs);
    }

    @Test
    public void nonKerberosProcedureKeepsTheLoadedTable(@TempDir Path temp) {
        Table table = createTable(temp);
        // A null plugin authenticator is a non-Kerberos catalog; a plain context is an offline test or fe-core fake.
        IcebergProcedureOps nonKerberos = new IcebergProcedureOps(Collections.emptyMap(), opsReturning(table),
                new TcclPinningConnectorContext(new RecordingConnectorContext(), getClass().getClassLoader(),
                        () -> null));
        IcebergProcedureOps plainContext = new IcebergProcedureOps(Collections.emptyMap(), opsReturning(table),
                new RecordingConnectorContext());

        Assertions.assertSame(table, nonKerberos.wrapTableForProcedure(table));
        Assertions.assertSame(table, plainContext.wrapTableForProcedure(table));
    }

    private static Table createTable(Path temp) {
        return new HadoopTables(new Configuration()).create(SCHEMA, PartitionSpec.unpartitioned(),
                Collections.emptyMap(), temp.resolve("table").toUri().toString());
    }

    private static void appendFile(Table table, Path path) throws IOException {
        table.newFastAppend().appendFile(dataFile(createOldFile(path))).commit();
    }

    private static DataFile dataFile(Path path) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path.toUri().toString())
                .withFileSizeInBytes(1)
                .withRecordCount(1)
                .build();
    }

    private static Path createOldFile(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[] {1});
        Files.setLastModifiedTime(path, FileTime.fromMillis(1));
        return path;
    }

    /** The same table, reopened over {@code io} so that every metadata file goes through it. */
    private static Table withFileIO(Table table, DoAsCheckingFileIO io) {
        HadoopTableOperations ops = new HadoopTableOperations(new org.apache.hadoop.fs.Path(table.location()), io,
                new Configuration(), LockManagers.defaultLockManager()) {
        };
        return new BaseTable(ops, table.name());
    }

    private static IcebergProcedureOps kerberosProcedureOps(Table table, ThreadMarkingAuthenticator auth) {
        TcclPinningConnectorContext context = new TcclPinningConnectorContext(
                new RecordingConnectorContext(), IcebergProcedureOpsKerberosIoTest.class.getClassLoader(), () -> auth);
        return new IcebergProcedureOps(Collections.emptyMap(), opsReturning(table), context);
    }

    private static RecordingIcebergCatalogOps opsReturning(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return ops;
    }

    /**
     * Runs the action inline and marks only the current thread as inside the doAs, as a Kerberos {@code Subject}
     * entered on one thread is invisible to iceberg's pool threads.
     */
    private static final class ThreadMarkingAuthenticator implements HadoopAuthenticator {
        private final ThreadLocal<Boolean> inDoAs = ThreadLocal.withInitial(() -> false);

        boolean inDoAs() {
            return inDoAs.get();
        }

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("doAs is overridden; no real UGI in this test");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            boolean outer = inDoAs.get();
            inDoAs.set(true);
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                inDoAs.set(outer);
            }
        }
    }

    /**
     * {@code HadoopFileIO} that records each call made outside the authenticator's doAs and counts the calls made
     * off the test thread.
     */
    private static final class DoAsCheckingFileIO implements SupportsPrefixOperations {
        private final HadoopFileIO delegate = new HadoopFileIO(new Configuration());
        private final ThreadMarkingAuthenticator auth;
        private final Thread testThread = Thread.currentThread();
        final List<String> callsOutsideDoAs = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger workerThreadCalls = new AtomicInteger();
        final AtomicInteger listPrefixCalls = new AtomicInteger();

        DoAsCheckingFileIO(ThreadMarkingAuthenticator auth) {
            this.auth = auth;
        }

        private void check(String op, String path) {
            if (Thread.currentThread() != testThread) {
                workerThreadCalls.incrementAndGet();
            }
            if (!auth.inDoAs()) {
                callsOutsideDoAs.add(op + " " + path + " on " + Thread.currentThread().getName());
            }
        }

        @Override
        public InputFile newInputFile(String path) {
            check("newInputFile", path);
            return delegate.newInputFile(path);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            check("newInputFile", path);
            return delegate.newInputFile(path, length);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            check("newOutputFile", path);
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            check("deleteFile", path);
            delegate.deleteFile(path);
        }

        @Override
        public Iterable<FileInfo> listPrefix(String prefix) {
            check("listPrefix", prefix);
            listPrefixCalls.incrementAndGet();
            return delegate.listPrefix(prefix);
        }

        @Override
        public void deletePrefix(String prefix) {
            check("deletePrefix", prefix);
            delegate.deletePrefix(prefix);
        }

        @Override
        public Map<String, String> properties() {
            return delegate.properties();
        }
    }
}
