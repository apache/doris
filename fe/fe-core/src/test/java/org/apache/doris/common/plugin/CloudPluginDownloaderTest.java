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

package org.apache.doris.common.plugin;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.plugin.CloudPluginDownloader.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudPluginDownloaderTest {

    @TempDir
    Path tempDir;

    @Test
    void testValidateInput() {
        Assertions.assertDoesNotThrow(() -> {
            CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, "mysql.jar");
            CloudPluginDownloader.validateInput(PluginType.JAVA_UDF, "nested/my_udf-1.0@prod.jar");
        });

        Assertions.assertEquals("Plugin name cannot be empty",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, ""))
                        .getMessage());
        Assertions.assertEquals("Plugin name cannot be empty",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, null))
                        .getMessage());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> CloudPluginDownloader.validateInput(PluginType.CONNECTORS, "test.jar"));

        for (String invalidName : new String[] {
                "../driver.jar", "nested/../../driver.jar", "nested/../driver.jar",
                "./driver.jar", "/driver.jar", "driver?.jar", "driver#v1.jar",
                "driver%20v1.jar", "nested\\driver.jar", "driver.txt"
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, invalidName),
                    invalidName);
        }
    }

    @Test
    void testDownloadRejectsTargetOutsidePluginDirectoryBeforeCloudAccess() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudPluginDownloader.downloadFromCloud(
                        PluginType.JDBC_DRIVERS, "driver.jar", tempDir.resolve("driver.jar").toString()));
    }

    @Test
    void testSelectLatestCloudStorageInfo() {
        Cloud.ObjectStoreInfoPB oldInfo = objectStoreInfo("old-bucket", "old-prefix");
        Cloud.ObjectStoreInfoPB latestInfo = objectStoreInfo("latest-bucket", "latest-prefix");
        Cloud.GetObjStoreInfoResponse response = responseBuilder()
                .addObjInfo(oldInfo)
                .addObjInfo(latestInfo)
                .build();

        Assertions.assertEquals(latestInfo, CloudPluginDownloader.selectCloudStorageInfo(response));
    }

    @Test
    void testRejectInvalidCloudStorageResponses() {
        Cloud.GetObjStoreInfoResponse failed = Cloud.GetObjStoreInfoResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                        .setMsg("test error"))
                .build();
        Assertions.assertTrue(Assertions.assertThrows(RuntimeException.class,
                () -> CloudPluginDownloader.selectCloudStorageInfo(failed)).getMessage()
                .contains("Failed to get storage info"));

        Assertions.assertTrue(Assertions.assertThrows(RuntimeException.class,
                () -> CloudPluginDownloader.selectCloudStorageInfo(responseBuilder().build())).getMessage()
                .contains("Only SaaS cloud storage is supported"));

        Cloud.GetObjStoreInfoResponse storageVault = responseBuilder()
                .setEnableStorageVault(true)
                .addObjInfo(objectStoreInfo("vault-bucket", "vault-prefix"))
                .build();
        Assertions.assertTrue(Assertions.assertThrows(RuntimeException.class,
                () -> CloudPluginDownloader.selectCloudStorageInfo(storageVault)).getMessage()
                .contains("legacy SaaS mode"));
    }

    @Test
    void testBuildS3Path() {
        Cloud.ObjectStoreInfoPB withPrefix = objectStoreInfo("test-bucket", "test-prefix");
        Assertions.assertEquals("s3://test-bucket/test-prefix/plugins/jdbc_drivers/mysql.jar",
                CloudPluginDownloader.buildS3Path(withPrefix, PluginType.JDBC_DRIVERS, "mysql.jar"));
        Assertions.assertEquals("s3://test-bucket/test-prefix/plugins/java_udf/nested/my_udf.jar",
                CloudPluginDownloader.buildS3Path(withPrefix, PluginType.JAVA_UDF, "nested/my_udf.jar"));

        Cloud.ObjectStoreInfoPB withoutPrefix = objectStoreInfo("test-bucket", null);
        Assertions.assertEquals("s3://test-bucket/plugins/java_udf/test-udf@v1.0.jar",
                CloudPluginDownloader.buildS3Path(withoutPrefix, PluginType.JAVA_UDF,
                        "test-udf@v1.0.jar"));
    }

    @Test
    void testDownloadPublishesCompleteFileAtomically() throws Exception {
        Path target = tempDir.resolve("driver.jar");
        Files.write(target, "old".getBytes(StandardCharsets.UTF_8));

        String result = CloudPluginDownloader.downloadToLocal(target,
                () -> new ByteArrayInputStream("new-driver".getBytes(StandardCharsets.UTF_8)));

        Assertions.assertEquals(target.toAbsolutePath().toString(), result);
        Assertions.assertEquals("new-driver", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        assertOnlyTargetRemains(target);
    }

    @Test
    void testDownloadFailurePreservesExistingFileAndClosesStream() throws Exception {
        Path target = tempDir.resolve("driver.jar");
        Files.write(target, "old-driver".getBytes(StandardCharsets.UTF_8));
        AtomicBoolean closed = new AtomicBoolean();

        Assertions.assertThrows(IOException.class, () -> CloudPluginDownloader.downloadToLocal(target,
                () -> failingStream(closed)));

        Assertions.assertTrue(closed.get());
        Assertions.assertEquals("old-driver", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
        assertOnlyTargetRemains(target);
    }

    @Test
    void testConcurrentDownloadsOfSameTargetAreSerialized() throws Exception {
        Path target = tempDir.resolve("driver.jar");
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<String> first = executor.submit(() -> CloudPluginDownloader.downloadToLocal(target, () -> {
                firstStarted.countDown();
                releaseFirst.await();
                return new ByteArrayInputStream("first".getBytes(StandardCharsets.UTF_8));
            }));
            Assertions.assertTrue(firstStarted.await(5, TimeUnit.SECONDS));

            Future<String> second = executor.submit(() -> CloudPluginDownloader.downloadToLocal(target, () -> {
                secondStarted.countDown();
                return new ByteArrayInputStream("second".getBytes(StandardCharsets.UTF_8));
            }));
            Assertions.assertFalse(secondStarted.await(200, TimeUnit.MILLISECONDS));

            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals("second", new String(Files.readAllBytes(target), StandardCharsets.UTF_8));
            assertOnlyTargetRemains(target);
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testDownloadsOfDifferentTargetsCanRunConcurrently() throws Exception {
        Path firstTarget = tempDir.resolve("first.jar");
        Path secondTarget = tempDir.resolve("second.jar");
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<String> first = executor.submit(() -> CloudPluginDownloader.downloadToLocal(
                    firstTarget, () -> {
                        firstStarted.countDown();
                        releaseFirst.await();
                        return new ByteArrayInputStream("first".getBytes(StandardCharsets.UTF_8));
                    }));
            Assertions.assertTrue(firstStarted.await(5, TimeUnit.SECONDS));

            Future<String> second = executor.submit(() -> CloudPluginDownloader.downloadToLocal(
                    secondTarget, () -> {
                        secondStarted.countDown();
                        return new ByteArrayInputStream("second".getBytes(StandardCharsets.UTF_8));
                    }));
            Assertions.assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
            second.get(5, TimeUnit.SECONDS);

            releaseFirst.countDown();
            first.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals("first",
                    new String(Files.readAllBytes(firstTarget), StandardCharsets.UTF_8));
            Assertions.assertEquals("second",
                    new String(Files.readAllBytes(secondTarget), StandardCharsets.UTF_8));
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    private static Cloud.GetObjStoreInfoResponse.Builder responseBuilder() {
        return Cloud.GetObjStoreInfoResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK));
    }

    private static Cloud.ObjectStoreInfoPB objectStoreInfo(String bucket, String prefix) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder()
                .setProvider(Cloud.ObjectStoreInfoPB.Provider.S3)
                .setBucket(bucket);
        if (prefix != null) {
            builder.setPrefix(prefix);
        }
        return builder.build();
    }

    private static InputStream failingStream(AtomicBoolean closed) {
        return new InputStream() {
            private boolean firstRead = true;

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                if (firstRead) {
                    firstRead = false;
                    bytes[offset] = 'x';
                    return 1;
                }
                throw new IOException("injected download failure");
            }

            @Override
            public int read() throws IOException {
                throw new IOException("injected download failure");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
    }

    private static void assertOnlyTargetRemains(Path target) throws IOException {
        try (java.util.stream.Stream<Path> files = Files.list(target.getParent())) {
            Assertions.assertArrayEquals(new Path[] {target}, files.toArray(Path[]::new));
        }
    }
}
