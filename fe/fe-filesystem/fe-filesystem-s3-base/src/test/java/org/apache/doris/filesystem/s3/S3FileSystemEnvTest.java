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

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;

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
 * Environment-dependent integration tests for {@link S3FileSystem}.
 * Tests FileSystem-level operations (exists, delete, rename, list, IO) using real S3.
 */
@Tag("environment")
@Tag("s3")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_S3_ENDPOINT", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class S3FileSystemEnvTest {

    private static final String PREFIX = "doris-fs-ut-" + UUID.randomUUID() + "/";
    private static S3FileSystem fs;
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
        fs = new S3FileSystem(new S3ObjStorage(props));
    }

    @AfterAll
    static void tearDown() throws IOException {
        // Clean up all objects under test prefix
        try (FileIterator iter = fs.list(loc(PREFIX))) {
            while (iter.hasNext()) {
                FileEntry e = iter.next();
                try {
                    fs.delete(e.location(), false);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        } catch (IOException ignored) {
            // prefix may not have any objects
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

    // ------------------------------------------------------------------
    // exists
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void existsReturnsTrueForExistingObject() throws IOException {
        writeContent("exists-test.txt", "data".getBytes());
        Assertions.assertTrue(fs.exists(loc("exists-test.txt")));
    }

    @Test
    @Order(2)
    void existsReturnsFalseForMissing() throws IOException {
        Assertions.assertFalse(fs.exists(loc("non-existent-" + UUID.randomUUID())));
    }

    // ------------------------------------------------------------------
    // delete
    // ------------------------------------------------------------------

    @Test
    @Order(3)
    void deleteRemovesObject() throws IOException {
        writeContent("delete-test.txt", "to-delete".getBytes());
        Assertions.assertTrue(fs.exists(loc("delete-test.txt")));

        fs.delete(loc("delete-test.txt"), false);
        Assertions.assertFalse(fs.exists(loc("delete-test.txt")));
    }

    // ------------------------------------------------------------------
    // rename
    // ------------------------------------------------------------------

    @Test
    @Order(4)
    void renameMovesObject() throws IOException {
        writeContent("rename-src.txt", "rename-data".getBytes());

        fs.rename(loc("rename-src.txt"), loc("rename-dst.txt"));

        Assertions.assertFalse(fs.exists(loc("rename-src.txt")));
        Assertions.assertTrue(fs.exists(loc("rename-dst.txt")));
    }

    // ------------------------------------------------------------------
    // list
    // ------------------------------------------------------------------

    @Test
    @Order(5)
    void listReturnsCorrectEntries() throws IOException {
        writeContent("list-dir/file1.csv", "aaa".getBytes());
        writeContent("list-dir/file2.csv", "bbb".getBytes());

        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator iter = fs.list(loc("list-dir/"))) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }

        Assertions.assertTrue(entries.size() >= 2,
                "Expected at least 2 entries, got " + entries.size());
    }

    /**
     * Regression test: prefix-based listing must not pull in objects under
     * sibling directories that share a string prefix (e.g. listing "store"
     * must not include "store_sales/..." or "store_returns/...").
     * Reproduces the tpcds external-table mis-scan bug.
     */
    @Test
    @Order(5)
    void listMustNotIncludeSiblingPrefixDirectories() throws IOException {
        writeContent("sibling-prefix/store/data.orc", "store-data".getBytes());
        writeContent("sibling-prefix/store_sales/poison.orc", "should-not-appear".getBytes());
        writeContent("sibling-prefix/store_returns/poison.orc", "should-not-appear".getBytes());

        // Note: location intentionally omits the trailing slash, mimicking how
        // an external table location is supplied by the user.
        Location target = loc("sibling-prefix/store");
        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator iter = fs.list(target)) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }

        for (FileEntry e : entries) {
            String uri = e.location().uri();
            Assertions.assertFalse(uri.contains("/store_sales/"),
                    "list of 'store' must not contain sibling 'store_sales' object: " + uri);
            Assertions.assertFalse(uri.contains("/store_returns/"),
                    "list of 'store' must not contain sibling 'store_returns' object: " + uri);
        }
        Assertions.assertEquals(1, entries.size(),
                "Expected exactly one object under 'store/', got: " + entries);
    }

    // ------------------------------------------------------------------
    // IO round-trip
    // ------------------------------------------------------------------

    @Test
    @Order(6)
    void inputOutputRoundTrip() throws IOException {
        byte[] expected = "Hello S3 round-trip test 你好世界 🚀".getBytes();
        writeContent("roundtrip.bin", expected);

        byte[] actual = readAll("roundtrip.bin");
        Assertions.assertArrayEquals(expected, actual);
    }

    // ------------------------------------------------------------------
    // inputFile length
    // ------------------------------------------------------------------

    @Test
    @Order(7)
    void inputFileLength() throws IOException {
        byte[] data = new byte[1234];
        java.util.Arrays.fill(data, (byte) 'X');
        writeContent("length-check.bin", data);

        DorisInputFile inputFile = fs.newInputFile(loc("length-check.bin"));
        Assertions.assertEquals(1234L, inputFile.length());
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
        byte[] payload = "s3-presigned-upload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
