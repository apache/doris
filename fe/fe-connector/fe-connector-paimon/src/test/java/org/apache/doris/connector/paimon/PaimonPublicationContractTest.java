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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.jdbc.JdbcCatalogLock;
import org.apache.paimon.jdbc.JdbcClientPool;
import org.apache.paimon.jdbc.JdbcUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** SDK contract probes: these intentionally demonstrate why unprotected publication is rejected. */
public class PaimonPublicationContractTest {
    @TempDir
    Path directory;

    @Test
    public void twoJvmLocalRenamesCanLoseAnAcknowledgedWrite() throws Exception {
        org.apache.paimon.fs.Path location = new org.apache.paimon.fs.Path(directory.resolve("tbl").toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), location).createTable(Schema.newBuilder()
                .column("id", DataTypes.INT()).option("bucket", "-1").build());
        ExecutorService reads = Executors.newSingleThreadExecutor();
        Process first = startWriter(location, 1);
        Process second = null;
        try {
            second = startWriter(location, 2);
            try (BufferedReader firstOutput = new BufferedReader(new InputStreamReader(first.getInputStream()));
                    BufferedReader secondOutput = new BufferedReader(new InputStreamReader(second.getInputStream()));
                    PrintWriter firstInput = new PrintWriter(first.getOutputStream(), true);
                    PrintWriter secondInput = new PrintWriter(second.getOutputStream(), true)) {
                // Both real LocalFileIO.rename calls have checked that snapshot-1 does not exist.
                Assertions.assertEquals("CHECKED", reads.submit(firstOutput::readLine).get(30, TimeUnit.SECONDS));
                Assertions.assertEquals("CHECKED", reads.submit(secondOutput::readLine).get(30, TimeUnit.SECONDS));
                firstInput.println("publish");
                Assertions.assertEquals("COMMITTED", reads.submit(firstOutput::readLine).get(30, TimeUnit.SECONDS));
                Assertions.assertEquals(List.of(1), readIds(location));
                secondInput.println("publish");
                Assertions.assertEquals("COMMITTED", reads.submit(secondOutput::readLine).get(30, TimeUnit.SECONDS));
                // The second acknowledged snapshot overwrote the first; row 1 is now unreachable.
                Assertions.assertEquals(List.of(2), readIds(location));
            }
        } finally {
            first.destroyForcibly();
            first.waitFor(10, TimeUnit.SECONDS);
            if (second != null) {
                second.destroyForcibly();
                second.waitFor(10, TimeUnit.SECONDS);
            }
            reads.shutdownNow();
        }
    }

    @Test
    public void expiredJdbcOwnerCanReleaseTheNewOwnersLock() throws Exception {
        String url = "jdbc:sqlite:" + directory.resolve("locks.db");
        ExecutorService writers = Executors.newFixedThreadPool(2);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch resumeFirst = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        CountDownLatch resumeSecond = new CountDownLatch(1);
        try (JdbcClientPool pool = new JdbcClientPool(3, url, Map.of())) {
            JdbcUtils.createDistributedLockTable(pool, new Options());
            JdbcCatalogLock first = new JdbcCatalogLock(pool, "catalog", 10, 60000);
            JdbcCatalogLock second = new JdbcCatalogLock(pool, "catalog", 10, 60000);
            var firstResult = writers.submit(() -> first.runWithLock("db", "tbl", () -> {
                firstEntered.countDown();
                Assertions.assertTrue(resumeFirst.await(30, TimeUnit.SECONDS));
                return true;
            }));
            Assertions.assertTrue(firstEntered.await(30, TimeUnit.SECONDS));
            // Move the lease into the past explicitly; no wall-clock sleeps are needed.
            try (var connection = DriverManager.getConnection(url);
                    var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE paimon_distributed_locks SET acquired_at='2000-01-01 00:00:00'");
            }
            var secondResult = writers.submit(() -> second.runWithLock("db", "tbl", () -> {
                secondEntered.countDown();
                Assertions.assertTrue(resumeSecond.await(30, TimeUnit.SECONDS));
                return true;
            }));
            Assertions.assertTrue(secondEntered.await(30, TimeUnit.SECONDS));
            resumeFirst.countDown();
            Assertions.assertTrue(firstResult.get(30, TimeUnit.SECONDS));
            // A third writer can acquire while writer 2 is still in its protected callback.
            Assertions.assertTrue(JdbcUtils.acquire(pool, "catalog.db.tbl", 60000));
            resumeSecond.countDown();
            Assertions.assertTrue(secondResult.get(30, TimeUnit.SECONDS));
        } finally {
            resumeFirst.countDown();
            resumeSecond.countDown();
            writers.shutdownNow();
        }
    }

    private Process startWriter(org.apache.paimon.fs.Path location, int id) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                LocalWriter.class.getName(), location.toString(), Integer.toString(id))
                .redirectError(directory.resolve("writer-" + id + ".log").toFile()).start();
    }

    private static List<Integer> readIds(org.apache.paimon.fs.Path location) throws Exception {
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), location);
        var builder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    public static class LocalWriter {
        public static void main(String[] args) throws Exception {
            FileStoreTable table = FileStoreTableFactory.create(new PausingLocalFileIO(),
                    new org.apache.paimon.fs.Path(args[0]));
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite(); var commit = builder.newCommit()) {
                write.write(GenericRow.of(Integer.parseInt(args[1])));
                commit.commit(write.prepareCommit());
            }
            System.out.println("COMMITTED");
        }
    }

    /** Pause the target existence check inside the SDK's real rename, after observing absence. */
    public static class PausingLocalFileIO extends LocalFileIO {
        private transient boolean renaming;

        @Override
        public boolean rename(org.apache.paimon.fs.Path source, org.apache.paimon.fs.Path target)
                throws java.io.IOException {
            renaming = true;
            try {
                return super.rename(source, target);
            } finally {
                renaming = false;
            }
        }

        @Override
        public File toFile(org.apache.paimon.fs.Path path) {
            File file = super.toFile(path);
            if (renaming && path.getName().equals("snapshot-1")) {
                return new File(file.toString()) {
                    @Override
                    public boolean exists() {
                        boolean exists = super.exists();
                        Assertions.assertFalse(exists);
                        System.out.println("CHECKED");
                        try {
                            Assertions.assertEquals("publish", new BufferedReader(
                                    new InputStreamReader(System.in)).readLine());
                        } catch (java.io.IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                        return exists;
                    }
                };
            }
            return file;
        }
    }
}
