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

package org.apache.doris.datasource.hive;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.local.LocalFileSystem;
import org.apache.doris.fs.SpiSwitchingFileSystem;
import org.apache.doris.qe.ConnectContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class HMSTransactionPathTest {
    private ConnectContext connectContext;

    @Before
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @After
    public void tearDown() {
        ConnectContext.remove();
        connectContext = null;
    }

    @Test
    public void testIsSubDirectory() throws Exception {
        Assert.assertFalse(HMSTransaction.isSubDirectory(null, "/a"));
        Assert.assertFalse(HMSTransaction.isSubDirectory("/a", null));
        Assert.assertFalse(HMSTransaction.isSubDirectory("/a/b", "/a/b"));
        Assert.assertFalse(HMSTransaction.isSubDirectory("/a/b", "/a/bc"));
        Assert.assertFalse(HMSTransaction.isSubDirectory(
                "hdfs://host1:8020/a", "hdfs://host2:8020/a/b"));
        Assert.assertTrue(HMSTransaction.isSubDirectory(
                "hdfs://host:8020/a/b/", "hdfs://host:8020/a/b/c/d"));
        Assert.assertTrue(HMSTransaction.isSubDirectory("a/b", "a/b/c"));
    }

    @Test
    public void testGetImmediateChildPath() throws Exception {
        String parent = "hdfs://host:8020/warehouse/table";
        String child = "hdfs://host:8020/warehouse/table/.doris_staging/user/uuid";
        Assert.assertEquals(
                "hdfs://host:8020/warehouse/table/.doris_staging",
                HMSTransaction.getImmediateChildPath(parent, child));

        String directChild = "hdfs://host:8020/warehouse/table/part=1";
        Assert.assertEquals(
                "hdfs://host:8020/warehouse/table/part=1",
                HMSTransaction.getImmediateChildPath(parent, directChild));

        String notSubdir = "hdfs://host:8020/warehouse/other";
        Assert.assertNull(HMSTransaction.getImmediateChildPath(parent, notSubdir));
    }

    // Ensures NOT_FOUND results from list operations are treated as no-op cleanup.
    @Test
    public void testDeleteTargetPathContentsNotFoundAllowed() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.listDirectoriesThrows = new IOException("not found");
        fakeFs.listFilesThrows = new IOException("not found");

        HMSTransaction transaction = createTransaction(fakeFs);
        Assert.assertThrows(RuntimeException.class, () -> transaction.deleteTargetPathContents(
                "/tmp/does_not_exist", "/tmp/does_not_exist/.doris_staging"));
    }

    // Verifies listDirectories failures surface as runtime errors.
    @Test
    public void testDeleteTargetPathContentsListError() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.listDirectoriesThrows = new IOException("list failed");

        HMSTransaction transaction = createTransaction(fakeFs);
        Assert.assertThrows(RuntimeException.class, () -> transaction.deleteTargetPathContents(
                "/tmp/target", "/tmp/target/.doris_staging"));
    }

    @Test
    public void testEnsureDirectorySuccess() throws Exception {
        LocalFileSystem localFs = new LocalFileSystem(Collections.emptyMap());
        HMSTransaction transaction = createTransaction(localFs);

        java.nio.file.Path dir = Files.createTempDirectory("hms_tx_ensure_").resolve("nested");
        transaction.ensureDirectory(dir.toString());

        Assert.assertTrue(Files.exists(dir));
    }

    @Test
    public void testEnsureDirectoryError() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.mkdirsThrows = new IOException("mkdir failed");

        HMSTransaction transaction = createTransaction(fakeFs);
        Assert.assertThrows(RuntimeException.class, () -> transaction.ensureDirectory("/tmp/target"));
    }

    // Verifies the staging-under-target flow:
    // 1) Detect write path nested under target.
    // 2) Compute the immediate staging root under target.
    // 3) Delete target contents while preserving the staging root.
    // 4) Ensure the target directory exists after cleanup.
    @Test
    public void testDeleteTargetPathContentsSkipsExcludedDir() throws Exception {
        LocalFileSystem localFs = new LocalFileSystem(Collections.emptyMap());
        HMSTransaction transaction = createTransaction(localFs);

        java.nio.file.Path targetDir = Files.createTempDirectory("hms_tx_path_test_");
        java.nio.file.Path stagingDir = targetDir.resolve(".doris_staging");
        java.nio.file.Path writeDir = stagingDir.resolve("user/uuid");
        java.nio.file.Path stagingFile = stagingDir.resolve("staging.tmp");
        java.nio.file.Path otherDir = targetDir.resolve("part=1");
        java.nio.file.Path otherFile = targetDir.resolve("data.txt");

        Files.createDirectories(stagingDir);
        Files.createDirectories(writeDir);
        Files.createFile(stagingFile);
        Files.createDirectories(otherDir);
        Files.createFile(otherFile);

        String targetPath = targetDir.toString();
        String writePath = writeDir.toString();
        Assert.assertTrue(HMSTransaction.isSubDirectory(targetPath, writePath));
        String stagingRoot = HMSTransaction.getImmediateChildPath(targetPath, writePath);
        transaction.deleteTargetPathContents(targetPath, stagingRoot);
        transaction.ensureDirectory(targetPath);

        Assert.assertTrue(Files.exists(stagingDir));
        Assert.assertTrue(Files.exists(stagingFile));
        Assert.assertFalse(Files.exists(otherDir));
        Assert.assertFalse(Files.exists(otherFile));
    }

    private static HMSTransaction createTransaction(FileSystem delegate) {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(delegate);
        return new HMSTransaction(null, spiFs, Runnable::run);
    }

    private static class FakeFileSystem implements FileSystem {
        IOException listDirectoriesThrows;
        IOException listFilesThrows;
        IOException mkdirsThrows;

        final List<String> deletedDirectories = new ArrayList<>();
        final List<String> deletedFiles = new ArrayList<>();

        @Override
        public Set<String> listDirectories(Location dir) throws IOException {
            if (listDirectoriesThrows != null) {
                throw listDirectoriesThrows;
            }
            return Collections.emptySet();
        }

        @Override
        public List<FileEntry> listFiles(Location dir) throws IOException {
            if (listFilesThrows != null) {
                throw listFilesThrows;
            }
            return Collections.emptyList();
        }

        @Override
        public void mkdirs(Location location) throws IOException {
            if (mkdirsThrows != null) {
                throw mkdirsThrows;
            }
        }

        @Override
        public void delete(Location location, boolean recursive) throws IOException {
            if (recursive) {
                deletedDirectories.add(location.uri());
            } else {
                deletedFiles.add(location.uri());
            }
        }

        @Override
        public boolean exists(Location location) throws IOException {
            return false;
        }

        @Override
        public void rename(Location src, Location dst) throws IOException {
        }

        @Override
        public FileIterator list(Location location) throws IOException {
            return new FileIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public FileEntry next() {
                    throw new java.util.NoSuchElementException();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public DorisInputFile newInputFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisInputFile newInputFile(Location location, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisOutputFile newOutputFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
        }
    }
}
