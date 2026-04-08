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

package org.apache.doris.filesystem.azure;

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
 * Environment-dependent integration tests for {@link AzureFileSystem}.
 * Requires real Azure Blob Storage credentials via environment variables.
 */
@Tag("environment")
@Tag("azure")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_AZURE_ACCOUNT", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AzureFileSystemEnvTest {

    private static final String PREFIX = "doris-az-ut-" + UUID.randomUUID() + "/";
    private static AzureFileSystem fs;
    private static String container;
    private static String account;

    @BeforeAll
    static void setUp() {
        account = requireEnv("DORIS_FS_TEST_AZURE_ACCOUNT");
        container = requireEnv("DORIS_FS_TEST_AZURE_CONTAINER");
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", account);
        props.put("AZURE_ACCOUNT_KEY", requireEnv("DORIS_FS_TEST_AZURE_KEY"));
        props.put("AZURE_CONTAINER", container);
        fs = new AzureFileSystem(new AzureObjStorage(props));
    }

    @AfterAll
    static void tearDown() throws IOException {
        try (FileIterator iter = fs.list(loc(PREFIX))) {
            while (iter.hasNext()) {
                FileEntry e = iter.next();
                try {
                    fs.delete(e.location(), false);
                } catch (IOException ignored) {
                    // best-effort
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
        // Azure uses wasbs:// scheme: wasbs://container@account.blob.core.windows.net/path
        return Location.of("wasbs://" + container + "@" + account
                + ".blob.core.windows.net/" + PREFIX + suffix);
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
    void existsReturnsTrueForExistingBlob() throws IOException {
        writeContent("exists.txt", "data".getBytes());
        Assertions.assertTrue(fs.exists(loc("exists.txt")));
    }

    @Test
    @Order(2)
    void existsReturnsFalseForMissing() throws IOException {
        Assertions.assertFalse(fs.exists(loc("no-such-blob-" + UUID.randomUUID())));
    }

    @Test
    @Order(3)
    void deleteRemovesBlob() throws IOException {
        writeContent("to-delete.txt", "xxx".getBytes());
        Assertions.assertTrue(fs.exists(loc("to-delete.txt")));
        fs.delete(loc("to-delete.txt"), false);
        Assertions.assertFalse(fs.exists(loc("to-delete.txt")));
    }

    @Test
    @Order(4)
    void renameMovesBlob() throws IOException {
        writeContent("rename-src.txt", "rename".getBytes());
        fs.rename(loc("rename-src.txt"), loc("rename-dst.txt"));
        Assertions.assertFalse(fs.exists(loc("rename-src.txt")));
        Assertions.assertTrue(fs.exists(loc("rename-dst.txt")));
    }

    @Test
    @Order(5)
    void listReturnsEntries() throws IOException {
        writeContent("list/a.txt", "aaa".getBytes());
        writeContent("list/b.txt", "bbb".getBytes());

        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator iter = fs.list(loc("list/"))) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }
        Assertions.assertTrue(entries.size() >= 2);
    }

    @Test
    @Order(6)
    void inputOutputRoundTrip() throws IOException {
        byte[] expected = "Azure round-trip 你好 🌍".getBytes();
        writeContent("roundtrip.bin", expected);
        byte[] actual = readAll("roundtrip.bin");
        Assertions.assertArrayEquals(expected, actual);
    }

    @Test
    @Order(7)
    void inputFileLength() throws IOException {
        byte[] data = new byte[2048];
        java.util.Arrays.fill(data, (byte) 'Z');
        writeContent("length.bin", data);
        DorisInputFile inputFile = fs.newInputFile(loc("length.bin"));
        Assertions.assertEquals(2048L, inputFile.length());
    }

    @Test
    @Order(8)
    void getPresignedUrl_returnsValidUrlAndUploadWorks() throws IOException {
        // Azure getPresignedUrl expects a full Azure URI
        String azureKey = "wasbs://" + container + "@" + account
                + ".blob.core.windows.net/" + PREFIX + "presigned-put.txt";
        String presignedUrl = fs.getPresignedUrl(azureKey);
        Assertions.assertNotNull(presignedUrl);

        java.net.URL url = new java.net.URL(presignedUrl);
        Assertions.assertTrue(url.getProtocol().startsWith("http"));

        // Upload data through the presigned (SAS) URL
        byte[] payload = "azure-presigned-upload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(payload.length));
        conn.setRequestProperty("x-ms-blob-type", "BlockBlob");
        try (java.io.OutputStream os = conn.getOutputStream()) {
            os.write(payload);
        }
        int responseCode = conn.getResponseCode();
        conn.disconnect();
        Assertions.assertTrue(responseCode >= 200 && responseCode < 300,
                "PUT via presigned SAS URL should succeed, got HTTP " + responseCode);

        // Verify the object exists
        Assertions.assertTrue(fs.exists(loc("presigned-put.txt")));
    }
}
