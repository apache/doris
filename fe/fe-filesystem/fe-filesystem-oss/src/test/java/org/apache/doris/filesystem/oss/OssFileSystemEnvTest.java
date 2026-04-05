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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.s3.S3FileSystem;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Environment-dependent integration tests for OSS (Alibaba Cloud) via {@link S3FileSystem}.
 */
@Tag("environment")
@Tag("oss")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_OSS_ENDPOINT", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OssFileSystemEnvTest {

    private static final String PREFIX = "doris-oss-ut-" + UUID.randomUUID() + "/";
    private static S3FileSystem fs;
    private static String bucket;

    @BeforeAll
    static void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", requireEnv("DORIS_FS_TEST_OSS_ENDPOINT"));
        props.put("AWS_REGION", requireEnv("DORIS_FS_TEST_OSS_REGION"));
        props.put("AWS_BUCKET", requireEnv("DORIS_FS_TEST_OSS_BUCKET"));
        props.put("AWS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_OSS_AK"));
        props.put("AWS_SECRET_KEY", requireEnv("DORIS_FS_TEST_OSS_SK"));
        bucket = props.get("AWS_BUCKET");
        fs = new S3FileSystem(new OssObjStorage(props));
    }

    @AfterAll
    static void tearDown() throws IOException {
        try (FileIterator iter = fs.list(loc(""))) {
            while (iter.hasNext()) {
                try {
                    fs.delete(iter.next().location(), false);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        } catch (IOException ignored) {
            // no objects to clean
        }
        fs.close();
    }

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return val;
    }

    private static Location loc(String suffix) {
        return Location.of("s3://" + bucket + "/" + PREFIX + suffix);
    }

    private void writeContent(String suffix, byte[] data) throws IOException {
        DorisOutputFile out = fs.newOutputFile(loc(suffix));
        try (OutputStream os = out.createOrOverwrite()) {
            os.write(data);
        }
    }

    private byte[] readAll(String suffix) throws IOException {
        DorisInputFile in = fs.newInputFile(loc(suffix));
        try (DorisInputStream is = in.newStream()) {
            return is.readAllBytes();
        }
    }

    @Test
    @Order(1)
    void existsReturnsTrueForExisting() throws IOException {
        writeContent("exists.txt", "data".getBytes());
        Assertions.assertTrue(fs.exists(loc("exists.txt")));
    }

    @Test
    @Order(2)
    void existsReturnsFalseForMissing() throws IOException {
        Assertions.assertFalse(fs.exists(loc("missing-" + UUID.randomUUID())));
    }

    @Test
    @Order(3)
    void deleteRemovesObject() throws IOException {
        writeContent("del.txt", "x".getBytes());
        fs.delete(loc("del.txt"), false);
        Assertions.assertFalse(fs.exists(loc("del.txt")));
    }

    @Test
    @Order(4)
    void renameMovesObject() throws IOException {
        writeContent("src.txt", "r".getBytes());
        fs.rename(loc("src.txt"), loc("dst.txt"));
        Assertions.assertFalse(fs.exists(loc("src.txt")));
        Assertions.assertTrue(fs.exists(loc("dst.txt")));
    }

    @Test
    @Order(5)
    void listReturnsEntries() throws IOException {
        writeContent("l/a.csv", "a".getBytes());
        writeContent("l/b.csv", "b".getBytes());

        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator iter = fs.list(loc("l/"))) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }
        Assertions.assertTrue(entries.size() >= 2);
    }

    @Test
    @Order(6)
    void inputOutputRoundTrip() throws IOException {
        byte[] expected = "OSS round-trip 阿里云".getBytes();
        writeContent("rt.bin", expected);
        Assertions.assertArrayEquals(expected, readAll("rt.bin"));
    }

    @Test
    @Order(7)
    void inputFileLength() throws IOException {
        byte[] data = new byte[777];
        java.util.Arrays.fill(data, (byte) 'O');
        writeContent("len.bin", data);
        Assertions.assertEquals(777L, fs.newInputFile(loc("len.bin")).length());
    }

    @Test
    @Order(8)
    void getPresignedUrl_returnsValidUrlAndUploadWorks() throws IOException {
        String key = PREFIX + "presigned-put.txt";
        String presignedUrl = fs.getPresignedUrl(key);
        Assertions.assertNotNull(presignedUrl);

        java.net.URL url = new java.net.URL(presignedUrl);
        Assertions.assertTrue(url.getProtocol().startsWith("http"));

        // Upload data through the presigned URL
        byte[] payload = "oss-presigned-upload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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

        // Verify the object exists
        Assertions.assertTrue(fs.exists(loc("presigned-put.txt")));
    }
}
