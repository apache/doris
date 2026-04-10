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

package org.apache.doris.backup;

import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.foundation.fs.FsStorageType;
import org.apache.doris.fs.FileSystemDescriptor;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.service.FrontendOptions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


public class RepositoryTest {

    private Repository repo;
    private long repoId = 10000;
    private String name = "repo";
    private String location = "bos://backup-cmy";
    private String brokerName = "broker";

    private SnapshotInfo info;

    private org.apache.doris.filesystem.FileSystem mockFs = Mockito.mock(org.apache.doris.filesystem.FileSystem.class);
    private DorisOutputFile mockOutputFile = Mockito.mock(DorisOutputFile.class);
    private DorisInputFile mockInputFile = Mockito.mock(DorisInputFile.class);
    private Env mockedEnv = Mockito.mock(Env.class);
    private BrokerMgr mockedBrokerMgr = Mockito.mock(BrokerMgr.class);

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<FrontendOptions> mockedFrontendOptions;
    private MockedStatic<FileSystemFactory> mockedFileSystemFactory;

    private final BrokerProperties testProps = BrokerProperties.of("broker", Maps.newHashMap());

    @Before
    public void setUp() throws Exception {
        List<String> files = Lists.newArrayList();
        files.add("1.dat");
        files.add("1.hdr");
        files.add("1.idx");
        info = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/path/to/tablet/snapshot/", files);

        mockedFrontendOptions = Mockito.mockStatic(FrontendOptions.class);
        mockedFrontendOptions.when(FrontendOptions::getLocalHostAddress).thenReturn("127.0.0.1");

        mockedFileSystemFactory = Mockito.mockStatic(FileSystemFactory.class);
        mockedFileSystemFactory.when(() -> FileSystemFactory.getFileSystem(Mockito.anyMap())).thenReturn(mockFs);
        mockedFileSystemFactory.when(() -> FileSystemFactory.getFileSystem(Mockito.any(StorageProperties.class))).thenReturn(mockFs);
        mockedFileSystemFactory.when(() -> FileSystemFactory.getBrokerFileSystem(
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyMap())).thenReturn(mockFs);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(mockedEnv);

        Mockito.when(mockedEnv.getBrokerMgr()).thenReturn(mockedBrokerMgr);
        Mockito.when(mockedBrokerMgr.getBroker(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(new FsBroker("10.74.167.16", 8111));

        // initRepository() and ping() short-circuit when runningUnitTest is true,
        // which avoids real filesystem calls in those particular tests.
        FeConstants.runningUnitTest = true;
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        if (mockedFrontendOptions != null) {
            mockedFrontendOptions.close();
        }
        if (mockedFileSystemFactory != null) {
            mockedFileSystemFactory.close();
        }
    }

    @Test
    public void testGet() {
        repo = new Repository(10000, "repo", false, location, testProps);

        Assert.assertEquals(repoId, repo.getId());
        Assert.assertEquals(name, repo.getName());
        Assert.assertEquals(false, repo.isReadOnly());
        Assert.assertEquals(location, repo.getLocation());
        Assert.assertEquals(null, repo.getErrorMsg());
        Assert.assertTrue(System.currentTimeMillis() - repo.getCreateTime() < 1000);
    }

    @Test
    public void testInit() throws UserException {
        repo = new Repository(10000, "repo", false, location, testProps);

        // initRepository() short-circuits with OK when FeConstants.runningUnitTest == true
        Status st = repo.initRepository();
        System.out.println(st);
        Assert.assertTrue(st.ok());
    }

    @Test
    public void testassemnblePath() throws MalformedURLException, URISyntaxException {
        repo = new Repository(10000, "repo", false, location, testProps);

        // job info
        String label = "label";
        String createTime = "2018-04-12 20:46:45";
        String createTime2 = "2018-04-12-20-46-45";
        Timestamp ts = Timestamp.valueOf(createTime);
        long creastTs = ts.getTime();

        // "location/__palo_repository_repo_name/__ss_my_sp1/__info_2018-01-01-08-00-00"
        String expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.PREFIX_JOB_INFO + createTime2;
        Assert.assertEquals(expected, repo.assembleJobInfoFilePath(label, creastTs));

        // meta info
        expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.FILE_META_INFO;
        Assert.assertEquals(expected, repo.assembleMetaInfoFilePath(label));

        // snapshot path
        // /location/__palo_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10032/__10023/__3481721
        expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + "__ss_content/__db_1/__tbl_2/__part_3/__idx_4/__5/__7";
        Assert.assertEquals(expected, repo.assembleRemoteSnapshotPath(label, info));

        String rootTabletPath = "/__db_10000/__tbl_10001/__part_10002/_idx_10001/__10003";
        String path = repo.getRepoPath(label, rootTabletPath);
        Assert.assertEquals("bos://backup-cmy/__palo_repository_repo/__ss_label/__ss_content/__db_10000/__tbl_10001/__part_10002/_idx_10001/__10003",
                path);
    }

    @Test
    public void testPing() {
        repo = new Repository(10000, "repo", false, location, testProps);
        // ping() short-circuits with true when FeConstants.runningUnitTest == true
        Assert.assertTrue(repo.ping());
        Assert.assertTrue(repo.getErrorMsg() == null);
    }

    @Test
    public void testListSnapshots() throws IOException {
        // Return one snapshot directory (__ss_a) and one non-directory (_ss_b).
        // Only the directory with the correct prefix should appear in the result.
        Mockito.when(mockFs.list(Mockito.any(Location.class))).thenAnswer(inv -> {
            List<FileEntry> entries = Lists.newArrayList(
                    new FileEntry(
                            Location.of(location + "/__palo_repository_repo/"
                                    + Repository.PREFIX_SNAPSHOT_DIR + "a"),
                            100, true, 0L, null),
                    new FileEntry(
                            Location.of(location + "/__palo_repository_repo/_ss_b"),
                            100, false, 0L, null));
            return new FileIterator() {
                private int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < entries.size();
                }

                @Override
                public FileEntry next() throws IOException {
                    return entries.get(idx++);
                }

                @Override
                public void close() {
                }
            };
        });

        repo = new Repository(10000, "repo", false, location, testProps);
        List<String> snapshotNames = Lists.newArrayList();
        Status st = repo.listSnapshots(snapshotNames);
        Assert.assertTrue(st.ok());
        Assert.assertEquals(1, snapshotNames.size());
        Assert.assertEquals("a", snapshotNames.get(0));
    }

    /**
     * Tests that listSnapshots correctly extracts snapshot names from flat S3 object keys
     * that contain nested {@code __ss_content} segments. Before the fix, {@code lastIndexOf}
     * matched the later {@code /__ss_content} instead of the earlier {@code /__ss_snap1},
     * producing a spurious "content" snapshot name.
     */
    @Test
    public void testListSnapshotsFlatObjectKeys() throws IOException {
        String repoRoot = location + "/__palo_repository_repo";
        Mockito.when(mockFs.list(Mockito.any(Location.class))).thenAnswer(inv -> {
            List<FileEntry> entries = Lists.newArrayList(
                    // A meta file directly under __ss_snap1 (single __ss_ segment)
                    new FileEntry(
                            Location.of(repoRoot + "/__ss_snap1/__meta__abc"),
                            50, false, 0L, null),
                    // A data file under __ss_snap1/__ss_content (two __ss_ segments)
                    new FileEntry(
                            Location.of(repoRoot
                                    + "/__ss_snap1/__ss_content/__db_1/__tbl_2/data.dat"),
                            200, false, 0L, null),
                    // A second snapshot
                    new FileEntry(
                            Location.of(repoRoot + "/__ss_snap2/__meta__def"),
                            50, false, 0L, null),
                    // An entry without __ss_ prefix — should be skipped
                    new FileEntry(
                            Location.of(repoRoot + "/__repo_info"),
                            10, false, 0L, null));
            return new FileIterator() {
                private int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < entries.size();
                }

                @Override
                public FileEntry next() throws IOException {
                    return entries.get(idx++);
                }

                @Override
                public void close() {
                }
            };
        });

        repo = new Repository(10000, "repo", false, location, testProps);
        List<String> snapshotNames = Lists.newArrayList();
        Status st = repo.listSnapshots(snapshotNames);
        Assert.assertTrue(st.ok());
        Assert.assertEquals(2, snapshotNames.size());
        Assert.assertTrue(snapshotNames.contains("snap1"));
        Assert.assertTrue(snapshotNames.contains("snap2"));
        // "content" must NOT appear — it is a nested directory, not a snapshot
        Assert.assertFalse(snapshotNames.contains("content"));
    }

    @Test
    public void testUpload() throws IOException {
        // BROKER path: delete(tmp), upload via newOutputFile, rename, delete(final) are called
        Mockito.doNothing().when(mockFs).delete(Mockito.any(Location.class), Mockito.anyBoolean());
        Mockito.when(mockFs.newOutputFile(Mockito.any(Location.class))).thenReturn(mockOutputFile);
        Mockito.when(mockOutputFile.create()).thenReturn(new ByteArrayOutputStream());
        Mockito.doNothing().when(mockFs).rename(Mockito.any(Location.class), Mockito.any(Location.class));

        repo = new Repository(10000, "repo", false, location, testProps);
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        try (PrintWriter out = new PrintWriter(localFilePath)) {
            out.print("a");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            String remoteFilePath = location + "/remote_file";
            Status st = repo.upload(localFilePath, remoteFilePath);
            Assert.assertTrue(st.ok());
        } finally {
            File file = new File(localFilePath);
            file.delete();
        }
    }

    @Test
    public void testDownload() throws Exception {
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        File localFile = new File(localFilePath);
        try {
            try (PrintWriter out = new PrintWriter(localFile)) {
                out.print("a");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }

            // The remote file has an md5 checksum suffix matching content "a"
            // Broker does not support globListWithLimit; fall back to listFiles.
            Mockito.when(mockFs.globListWithLimit(Mockito.any(Location.class), Mockito.anyString(),
                    Mockito.anyLong(), Mockito.anyLong()))
                    .thenThrow(new UnsupportedOperationException("broker does not support globListWithLimit"));

            Mockito.when(mockFs.listFiles(Mockito.any(Location.class)))
                    .thenReturn(Lists.newArrayList(
                            new FileEntry(
                                    Location.of(location + "/remote_file"
                                            + ".0cc175b9c0f1b6a831c399e269772661"),
                                    1, false, 0L, null)));

            Mockito.when(mockFs.newInputFile(Mockito.any(Location.class))).thenReturn(mockInputFile);

            // Return a DorisInputStream wrapping content "a"
            Mockito.when(mockInputFile.newStream()).thenReturn(new DorisInputStream() {
                private final java.io.InputStream delegate =
                        new java.io.ByteArrayInputStream("a".getBytes());

                @Override
                public int read() throws IOException {
                    return delegate.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return delegate.read(b, off, len);
                }

                @Override
                public long getPos() throws IOException {
                    return 0;
                }

                @Override
                public void seek(long newPos) throws IOException {
                }
            });

            repo = new Repository(10000, "repo", false, location, testProps);
            String remoteFilePath = location + "/remote_file";
            Status st = repo.download(remoteFilePath, localFilePath);
            Assert.assertTrue(st.ok());
        } finally {
            localFile.delete();
        }
    }

    @Test
    public void testGetSnapshotInfo() throws IOException {
        // listSnapshots calls fs.list; return two snapshot directories
        Mockito.when(mockFs.list(Mockito.any(Location.class))).thenAnswer(inv -> {
            List<FileEntry> entries = Lists.newArrayList(
                    new FileEntry(
                            Location.of(location + "/__palo_repository_repo/"
                                    + Repository.PREFIX_SNAPSHOT_DIR + "s1"),
                            100, true, 0L, null),
                    new FileEntry(
                            Location.of(location + "/__palo_repository_repo/"
                                    + Repository.PREFIX_SNAPSHOT_DIR + "s2"),
                            100, true, 0L, null));
            return new FileIterator() {
                private int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < entries.size();
                }

                @Override
                public FileEntry next() throws IOException {
                    return entries.get(idx++);
                }

                @Override
                public void close() {
                }
            };
        });

        // getSnapshotInfo calls fs.listFiles to find __info_* files
        Mockito.when(mockFs.listFiles(Mockito.any(Location.class)))
                .thenReturn(Lists.newArrayList(
                        new FileEntry(
                                Location.of(location + "/__palo_repository_repo/__ss_s1/"
                                        + "__info_2018-04-18-20-11-00"
                                        + ".12345678123456781234567812345678"),
                                100, false, 0L, null)));

        repo = new Repository(10000, "repo", false, location, testProps);
        String snapshotName = "";
        String timestamp = "";
        try {
            List<List<String>> infos = repo.getSnapshotInfos(snapshotName, timestamp);
            Assert.assertEquals(2, infos.size());

        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Ignore("wait support")
    @Test
    public void testPersist() throws UserException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("bos_endpoint", "http://gz.bcebos.com");
        properties.put("bos_accesskey", "a");
        properties.put("bos_secret_accesskey", "b");
        repo = new Repository(10000, "repo", false, location, StorageProperties.createPrimary(properties));

        File file = new File("./Repository");
        try {
            DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
            repo.write(out);
            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
            Repository newRepo = Repository.read(in);
            in.close();

            Assert.assertEquals(repo.getName(), newRepo.getName());
            Assert.assertEquals(repo.getId(), newRepo.getId());
            Assert.assertEquals(repo.getLocation(), newRepo.getLocation());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail();
        } finally {
            file.delete();
        }
    }

    @Test
    public void testPathNormalize() {

        String newLoc = "bos://cmy_bucket/bos_repo/";
        repo = new Repository(10000, "repo", false, newLoc, testProps);
        String path = repo.getRepoPath("label1", "/_ss_my_ss/_ss_content/__db_10000/");
        Assert.assertEquals("bos://cmy_bucket/bos_repo/__palo_repository_repo/__ss_label1/__ss_content/_ss_my_ss/_ss_content/__db_10000/", path);

        path = repo.getRepoPath("label1", "/_ss_my_ss/_ss_content///__db_10000");
        Assert.assertEquals("bos://cmy_bucket/bos_repo/__palo_repository_repo/__ss_label1/__ss_content/_ss_my_ss/_ss_content/__db_10000", path);

        newLoc = "hdfs://path/to/repo";
        repo = new Repository(10000, "repo", false, newLoc, testProps);
        SnapshotInfo snapshotInfo = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/path", Lists.newArrayList());
        path = repo.getRepoTabletPathBySnapshotInfo("label1", snapshotInfo);
        Assert.assertEquals("hdfs://path/to/repo/__palo_repository_repo/__ss_label1/__ss_content/__db_1/__tbl_2/__part_3/__idx_4/__5", path);
    }

    /**
     * H1: Verify that a Repository serialized in the pre-SPI format (where the filesystem was
     * stored as {@code "fs": {"n":"brokerName","prop":{...}}}) is correctly migrated to a
     * {@link FileSystemDescriptor} during {@code gsonPostProcess()}.
     *
     * <p>Covers the backward-compatible deserialization path added as the H1 fix.
     */
    @Test
    public void testGsonPostProcessLegacyBrokerFormat() {
        // JSON shape produced by old PersistentFileSystem serialization:
        // "fs": { "n": "<brokerName>", "prop": {} }
        // No "fs_descriptor" field present.
        String legacyJson = "{"
                + "\"id\":10000,"
                + "\"name\":\"legacyRepo\","
                + "\"read_only\":false,"
                + "\"location\":\"bos://backup/legacy\","
                + "\"create_time\":-1,"
                + "\"fs\":{\"n\":\"broker\",\"prop\":{}}"
                + "}";

        // GsonUtils.GSON triggers gsonPostProcess() automatically via PostProcessTypeAdapterFactory.
        // FileSystemFactory is already mocked in setUp() to return mockFs for non-broker types.
        Repository deserialized = GsonUtils.GSON.fromJson(legacyJson, Repository.class);

        // The migration must produce a non-null FileSystemDescriptor.
        FileSystemDescriptor fd = deserialized.getFileSystemDescriptor();
        Assert.assertNotNull("fileSystemDescriptor must be migrated from legacy 'fs' field", fd);
        // Broker fallback is expected: props are empty so no primary storage type matches.
        Assert.assertEquals(FsStorageType.BROKER, fd.getStorageType());
        Assert.assertEquals("broker", fd.getName());
    }

    /**
     * H1: Verify migration when legacy props contain HDFS-specific keys that are recognized by
     * {@link StorageProperties#createPrimary(java.util.Map)} (primary path, not broker fallback).
     */
    @Test
    public void testGsonPostProcessLegacyHdfsFormat() {
        // "hdfs.authentication.type" triggers HdfsProperties.guessIsMe() → primary storage detection succeeds.
        // Deliberately avoids "dfs.nameservices" to skip HA validation in initNormalizeAndCheckProps().
        String legacyJson = "{"
                + "\"id\":20000,"
                + "\"name\":\"hdfsRepo\","
                + "\"read_only\":false,"
                + "\"location\":\"hdfs://ns/backup\","
                + "\"create_time\":-1,"
                + "\"fs\":{\"n\":\"\",\"prop\":{\"hdfs.authentication.type\":\"simple\"}}"
                + "}";

        Repository deserialized = GsonUtils.GSON.fromJson(legacyJson, Repository.class);

        FileSystemDescriptor fd = deserialized.getFileSystemDescriptor();
        Assert.assertNotNull("fileSystemDescriptor must be migrated from legacy HDFS 'fs' field", fd);
        Assert.assertEquals(FsStorageType.HDFS, fd.getStorageType());
    }

    /**
     * M2: {@code acquireSpiFs()} must pass {@code FrontendOptions.getLocalHostAddress()} to
     * {@link BrokerMgr#getBroker(String, String)} when resolving the broker endpoint.
     *
     * <p>The setUp() already mocks {@code FrontendOptions.getLocalHostAddress()} to return
     * {@code "127.0.0.1"}.  This test captures the actual
     * host argument forwarded to {@code getBroker()} and asserts it equals the mocked value.
     */
    @Test
    public void testAcquireSpiFsBrokerUsesLocalHostAddress() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> capturedHost =
                new java.util.concurrent.atomic.AtomicReference<>();

        // Override the setUp() stub for getBroker() with an answer that captures
        // the host argument so we can assert it equals FrontendOptions.getLocalHostAddress().
        Mockito.when(mockedBrokerMgr.getBroker(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(inv -> {
                    String host = inv.getArgument(1);
                    capturedHost.set(host);
                    return new FsBroker("10.74.167.16", 8111);
                });

        // listSnapshots() calls fs.list(); return an empty iterator so the method
        // completes normally without further interaction.
        Mockito.when(mockFs.list(Mockito.any(Location.class))).thenAnswer(inv -> {
            return new FileIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public FileEntry next() throws IOException {
                    throw new java.util.NoSuchElementException();
                }

                @Override
                public void close() {
                }
            };
        });

        repo = new Repository(10000, "repo", false, location, testProps);
        List<String> snapshotNames = Lists.newArrayList();
        repo.listSnapshots(snapshotNames); // triggers acquireSpiFs() → getBroker(name, host)

        Assert.assertNotNull(
                "getBroker() must have been called during listSnapshots()", capturedHost.get());
        Assert.assertEquals(
                "acquireSpiFs() must pass FrontendOptions.getLocalHostAddress() to getBroker()",
                "127.0.0.1", capturedHost.get());
    }

}
