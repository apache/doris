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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.UploadPartResult;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Environment-dependent integration tests for {@link S3ObjStorage}.
 * Requires real S3-compatible credentials via environment variables.
 */
@Tag("environment")
@Tag("s3")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_S3_ENDPOINT", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3ObjStorageEnvTest {

    private static final String PREFIX = "doris-ut-" + UUID.randomUUID() + "/";
    private static S3ObjStorage storage;
    private static String bucket;

    @BeforeAll
    static void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", requireEnv("DORIS_FS_TEST_S3_ENDPOINT"));
        props.put("AWS_REGION", requireEnv("DORIS_FS_TEST_S3_REGION"));
        props.put("AWS_BUCKET", requireEnv("DORIS_FS_TEST_S3_BUCKET"));
        props.put("AWS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_S3_AK"));
        props.put("AWS_SECRET_KEY", requireEnv("DORIS_FS_TEST_S3_SK"));
        bucket = props.get("AWS_BUCKET");
        storage = new S3ObjStorage(props);
    }

    @AfterAll
    static void tearDown() throws IOException {
        // Clean up all objects under the test prefix
        RemoteObjects result = storage.listObjects(
                "s3://" + bucket + "/" + PREFIX, null);
        for (RemoteObject obj : result.getObjectList()) {
            try {
                storage.deleteObject("s3://" + bucket + "/" + obj.getKey());
            } catch (IOException ignored) {
                // best-effort cleanup
            }
        }
        storage.close();
    }

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return val;
    }

    private String uri(String suffix) {
        return "s3://" + bucket + "/" + PREFIX + suffix;
    }

    private void putSmallObject(String suffix, byte[] data) throws IOException {
        storage.putObject(uri(suffix),
                RequestBody.of(new ByteArrayInputStream(data), data.length));
    }

    // ------------------------------------------------------------------
    // putObject + headObject
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void putAndHeadObject() throws IOException {
        byte[] data = "hello-s3-env-test".getBytes();
        putSmallObject("head-test.txt", data);

        RemoteObject ro = storage.headObject(uri("head-test.txt"));
        Assertions.assertEquals(data.length, ro.getSize());
        Assertions.assertNotNull(ro.getEtag());
    }

    // ------------------------------------------------------------------
    // listObjects
    // ------------------------------------------------------------------

    @Test
    @Order(2)
    void listObjects_returnsUploadedFiles() throws IOException {
        putSmallObject("list/a.txt", "aaa".getBytes());
        putSmallObject("list/b.txt", "bbb".getBytes());

        RemoteObjects result = storage.listObjects(uri("list/"), null);

        Assertions.assertTrue(result.getObjectList().size() >= 2,
                "Expected at least 2 objects, got " + result.getObjectList().size());
    }

    // ------------------------------------------------------------------
    // copyObject + deleteObject
    // ------------------------------------------------------------------

    @Test
    @Order(3)
    void copyAndDeleteObject() throws IOException {
        putSmallObject("copy-src.txt", "copy-data".getBytes());

        // Copy
        storage.copyObject(uri("copy-src.txt"), uri("copy-dst.txt"));

        // Verify destination exists
        RemoteObject dst = storage.headObject(uri("copy-dst.txt"));
        Assertions.assertEquals("copy-data".length(), dst.getSize());

        // Delete both
        storage.deleteObject(uri("copy-src.txt"));
        storage.deleteObject(uri("copy-dst.txt"));

        // Verify they are gone
        Assertions.assertThrows(Exception.class, () -> storage.headObject(uri("copy-src.txt")));
        Assertions.assertThrows(Exception.class, () -> storage.headObject(uri("copy-dst.txt")));
    }

    // ------------------------------------------------------------------
    // Multipart upload
    // ------------------------------------------------------------------

    @Test
    @Order(4)
    void multipartUpload_completeSucceeds() throws IOException {
        String path = uri("multipart-complete.bin");
        String uploadId = storage.initiateMultipartUpload(path);
        Assertions.assertNotNull(uploadId);

        // S3 requires parts >= 5MB except the last. Use a single part for simplicity.
        byte[] partData = new byte[5 * 1024 * 1024 + 1];
        java.util.Arrays.fill(partData, (byte) 'A');
        UploadPartResult part1 = storage.uploadPart(path, uploadId, 1,
                RequestBody.of(new ByteArrayInputStream(partData), partData.length));

        byte[] lastPart = "last-part".getBytes();
        UploadPartResult part2 = storage.uploadPart(path, uploadId, 2,
                RequestBody.of(new ByteArrayInputStream(lastPart), lastPart.length));

        storage.completeMultipartUpload(path, uploadId, List.of(part1, part2));

        RemoteObject completed = storage.headObject(path);
        Assertions.assertEquals(partData.length + lastPart.length, completed.getSize());
    }

    @Test
    @Order(5)
    void abortMultipartUpload_leavesNoObject() throws IOException {
        String path = uri("multipart-abort.bin");
        String uploadId = storage.initiateMultipartUpload(path);
        Assertions.assertNotNull(uploadId);

        storage.abortMultipartUpload(path, uploadId);

        // The object should not exist
        Assertions.assertThrows(Exception.class, () -> storage.headObject(path));
    }

    @Test
    @Order(6)
    void getPresignedUrl_returnsValidUrlAndUploadWorks() throws IOException {
        String key = PREFIX + "presigned-put.txt";
        String presignedUrl = storage.getPresignedUrl(key);
        Assertions.assertNotNull(presignedUrl);

        java.net.URL url = new java.net.URL(presignedUrl);
        Assertions.assertTrue(url.getProtocol().startsWith("http"));

        // Upload data through the presigned URL
        byte[] payload = "presigned-upload-test".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(payload.length));
        try (java.io.OutputStream os = conn.getOutputStream()) {
            os.write(payload);
        }
        int responseCode = conn.getResponseCode();
        conn.disconnect();
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300,
                "PUT via presigned URL should succeed, got HTTP " + responseCode);

        // Verify the object exists and has correct size
        String fullPath = "s3://" + bucket + "/" + key;
        RemoteObject obj = storage.headObject(fullPath);
        Assertions.assertEquals(payload.length, obj.getSize());
    }
}
