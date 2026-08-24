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

import org.apache.doris.backup.Status;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.LocalDfsFileSystem;
import org.apache.doris.fs.obj.ObjStorage;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.SwitchingFileSystem;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TS3MPUPendingUpload;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    public void testFinalReportRejectedAfterRollbackStarts() throws Exception {
        HMSTransaction transaction = createTransaction(Mockito.mock(FileSystem.class));
        THivePartitionUpdate update = new THivePartitionUpdate().setRowCount(1);
        CountDownLatch transactionFetched = new CountDownLatch(1);
        CountDownLatch resumeFinalReport = new CountDownLatch(1);
        AtomicReference<Throwable> reportFailure = new AtomicReference<>();
        Thread finalReport = new Thread(() -> {
            transactionFetched.countDown();
            try {
                resumeFinalReport.await();
                transaction.updateHivePartitionUpdates(Collections.singletonList(update));
            } catch (Throwable t) {
                reportFailure.set(t);
            }
        });
        finalReport.start();
        Assert.assertTrue(transactionFetched.await(5, TimeUnit.SECONDS));

        transaction.rollback();
        resumeFinalReport.countDown();
        finalReport.join(TimeUnit.SECONDS.toMillis(5));

        Assert.assertFalse(finalReport.isAlive());
        Assert.assertTrue(reportFailure.get() instanceof IllegalStateException);
        Assert.assertTrue(transaction.getHivePartitionUpdates().isEmpty());
    }

    // Ensures NOT_FOUND results from list operations are treated as no-op cleanup.
    @Test
    public void testDeleteTargetPathContentsNotFoundAllowed() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.listDirectoriesStatus = new Status(Status.ErrCode.NOT_FOUND, "missing");
        fakeFs.listFilesStatus = new Status(Status.ErrCode.NOT_FOUND, "missing");

        HMSTransaction transaction = createTransaction(fakeFs);
        transaction.deleteTargetPathContents(
                "/tmp/does_not_exist", "/tmp/does_not_exist/.doris_staging");
        Assert.assertTrue(fakeFs.deletedDirectories.isEmpty());
        Assert.assertTrue(fakeFs.deletedFiles.isEmpty());
    }

    // Verifies listDirectories failures surface as runtime errors.
    @Test
    public void testDeleteTargetPathContentsListError() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.listDirectoriesStatus = new Status(Status.ErrCode.COMMON_ERROR, "list failed");

        HMSTransaction transaction = createTransaction(fakeFs);
        Assert.assertThrows(RuntimeException.class, () -> transaction.deleteTargetPathContents(
                "/tmp/target", "/tmp/target/.doris_staging"));
    }

    @Test
    public void testEnsureDirectorySuccess() throws Exception {
        LocalDfsFileSystem localFs = new LocalDfsFileSystem();
        HMSTransaction transaction = createTransaction(localFs);

        java.nio.file.Path dir = Files.createTempDirectory("hms_tx_ensure_").resolve("nested");
        transaction.ensureDirectory(dir.toString());

        Assert.assertTrue(Files.exists(dir));
    }

    @Test
    public void testEnsureDirectoryError() throws Exception {
        FakeFileSystem fakeFs = new FakeFileSystem();
        fakeFs.makeDirStatus = new Status(Status.ErrCode.COMMON_ERROR, "mkdir failed");

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
        LocalDfsFileSystem localFs = new LocalDfsFileSystem();
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
        SwitchingFileSystem switchingFs = new TestSwitchingFileSystem(delegate);
        FileSystemProvider provider = ctx -> switchingFs;
        return new HMSTransaction(null, provider, Runnable::run);
    }

    private static class TestSwitchingFileSystem extends SwitchingFileSystem {
        private final FileSystem delegate;

        TestSwitchingFileSystem(FileSystem delegate) {
            super(null, null);
            this.delegate = delegate;
        }

        @Override
        public FileSystem fileSystem(String location) {
            return delegate;
        }
    }

    private static void setEmptyStagingDirectory(HMSTransaction tx) throws Exception {
        Field stagingDirField = HMSTransaction.class.getDeclaredField("stagingDirectory");
        stagingDirField.setAccessible(true);
        stagingDirField.set(tx, java.util.Optional.empty());
    }

    private static class FakeFileSystem implements FileSystem {
        private Status listDirectoriesStatus = Status.OK;
        private Status listFilesStatus = Status.OK;
        private Status makeDirStatus = Status.OK;
        private Status deleteStatus = Status.OK;
        private Status deleteDirStatus = Status.OK;

        private final List<String> deletedDirectories = new ArrayList<>();
        private final List<String> deletedFiles = new ArrayList<>();

        @Override
        public Status listDirectories(String remotePath, Set<String> result) {
            if (!listDirectoriesStatus.ok()) {
                return listDirectoriesStatus;
            }
            return Status.OK;
        }

        @Override
        public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
            if (!listFilesStatus.ok()) {
                return listFilesStatus;
            }
            return Status.OK;
        }

        @Override
        public Status deleteDirectory(String dir) {
            deletedDirectories.add(dir);
            return deleteDirStatus;
        }

        @Override
        public Status delete(String remotePath) {
            deletedFiles.add(remotePath);
            return deleteStatus;
        }

        @Override
        public Status makeDir(String remotePath) {
            return makeDirStatus;
        }

        @Override
        public Status exists(String remotePath) {
            return Status.OK;
        }

        @Override
        public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
            return Status.OK;
        }

        @Override
        public Status upload(String localPath, String remotePath) {
            return Status.OK;
        }

        @Override
        public Status directUpload(String content, String remoteFile) {
            return Status.OK;
        }

        @Override
        public Status rename(String origFilePath, String destFilePath) {
            return Status.OK;
        }

        @Override
        public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
            return Status.OK;
        }

        @Override
        public java.util.Map<String, String> getProperties() {
            return null;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRollbackAbortsPendingMpuBeforeCommitterCreated() throws Exception {
        S3Client s3Client = Mockito.mock(S3Client.class);
        ObjStorage<S3Client> storage = Mockito.mock(ObjStorage.class);
        Mockito.when(storage.getClient()).thenReturn(s3Client);
        S3FileSystem s3FileSystem = Mockito.mock(S3FileSystem.class);
        Mockito.doReturn(storage).when(s3FileSystem).getObjStorage();
        HMSTransaction tx = createTransaction(s3FileSystem);

        TS3MPUPendingUpload mpu = new TS3MPUPendingUpload();
        mpu.setBucket("test-bucket");
        mpu.setKey("warehouse/table/data-0.parquet");
        mpu.setUploadId("upload-id-1");

        THiveLocationParams location = new THiveLocationParams();
        location.setWritePath("s3://test-bucket/warehouse/table");

        THivePartitionUpdate update = new THivePartitionUpdate();
        update.setLocation(location);
        update.setFileNames(Collections.emptyList());
        update.setRowCount(0);
        update.setFileSize(0);
        update.setS3MpuPendingUploads(Collections.singletonList(mpu));
        tx.updateHivePartitionUpdates(Collections.singletonList(update));
        setEmptyStagingDirectory(tx);

        tx.rollback();

        ArgumentCaptor<AbortMultipartUploadRequest> request =
                ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        Mockito.verify(s3Client).abortMultipartUpload(request.capture());
        Assert.assertEquals("test-bucket", request.getValue().bucket());
        Assert.assertEquals("warehouse/table/data-0.parquet", request.getValue().key());
        Assert.assertEquals("upload-id-1", request.getValue().uploadId());
    }

    @Test
    public void testMergePreservesLaterMultipartRecordsAfterLegacyNullList() {
        HMSTransaction tx = createTransaction(Mockito.mock(FileSystem.class));
        THivePartitionUpdate legacy = new THivePartitionUpdate()
                .setName("part=1").setFileSize(1).setRowCount(1)
                .setFileNames(new ArrayList<>(Collections.singletonList("old.parquet")));
        TS3MPUPendingUpload upload = new TS3MPUPendingUpload()
                .setBucket("bucket").setKey("new.parquet").setUploadId("upload-id");
        THivePartitionUpdate current = new THivePartitionUpdate()
                .setName("part=1").setFileSize(2).setRowCount(2)
                .setFileNames(new ArrayList<>(Collections.singletonList("new.parquet")))
                .setS3MpuPendingUploads(new ArrayList<>(Collections.singletonList(upload)));

        THivePartitionUpdate merged = tx.mergePartitions(Arrays.asList(legacy, current)).get(0);

        Assert.assertEquals(Collections.singletonList(upload), merged.getS3MpuPendingUploads());
        Assert.assertEquals(Arrays.asList("old.parquet", "new.parquet"), merged.getFileNames());
    }

    @Test
    public void testObjectStoreCommitRequiresOneCompleteRecordPerFile() {
        HMSTransaction tx = createTransaction(Mockito.mock(FileSystem.class));
        tx.fileType = TFileType.FILE_S3;
        THivePartitionUpdate update = new THivePartitionUpdate()
                .setName("").setFileSize(1).setRowCount(1)
                .setFileNames(Collections.singletonList("data.parquet"));
        tx.updateHivePartitionUpdates(Collections.singletonList(update));

        IllegalStateException error = Assert.assertThrows(IllegalStateException.class,
                () -> tx.finishInsertTable(NameMapping.createForTest("db", "table")));

        Assert.assertTrue(error.getMessage().contains("reported 1 file(s)"));
    }
}
