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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Environment-dependent integration tests for {@link DFSFileSystem} using simple authentication.
 * Requires a running HDFS cluster accessible via environment variables.
 */
@Tag("environment")
@Tag("hdfs")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_HDFS_HOST", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DFSFileSystemEnvTest {

    private static final String BASE = "/doris-hdfs-ut-" + UUID.randomUUID();
    private static DFSFileSystem fs;
    private static String hdfsPrefix;

    @BeforeAll
    static void setUp() {
        String host = requireEnv("DORIS_FS_TEST_HDFS_HOST");
        String port = requireEnv("DORIS_FS_TEST_HDFS_PORT");
        hdfsPrefix = "hdfs://" + host + ":" + port;

        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", hdfsPrefix);
        props.put("hadoop.security.authentication", "simple");
        fs = new DFSFileSystem(props);
    }

    @AfterAll
    static void tearDown() throws IOException {
        try {
            fs.delete(loc(""), true);
        } catch (IOException ignored) {
            // best-effort cleanup
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
        return Location.of(hdfsPrefix + BASE + "/" + suffix);
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
    // mkdirs + exists
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    void mkdirsAndExists() throws IOException {
        fs.mkdirs(loc("subdir/deep/nested"));
        Assertions.assertTrue(fs.exists(loc("subdir/deep/nested")));
    }

    // ------------------------------------------------------------------
    // delete
    // ------------------------------------------------------------------

    @Test
    @Order(2)
    void deleteRecursive() throws IOException {
        fs.mkdirs(loc("del-dir"));
        writeContent("del-dir/file.txt", "data".getBytes());
        Assertions.assertTrue(fs.exists(loc("del-dir/file.txt")));

        fs.delete(loc("del-dir"), true);
        Assertions.assertFalse(fs.exists(loc("del-dir")));
    }

    // ------------------------------------------------------------------
    // rename
    // ------------------------------------------------------------------

    @Test
    @Order(3)
    void renameFile() throws IOException {
        writeContent("rename-src.txt", "rename-data".getBytes());
        fs.rename(loc("rename-src.txt"), loc("rename-dst.txt"));

        Assertions.assertFalse(fs.exists(loc("rename-src.txt")));
        Assertions.assertTrue(fs.exists(loc("rename-dst.txt")));
    }

    @Test
    @Order(4)
    void renameDirectory() throws IOException {
        fs.mkdirs(loc("rename-dir-src"));
        writeContent("rename-dir-src/inner.txt", "inside".getBytes());

        boolean[] callbackCalled = {false};
        fs.renameDirectory(loc("rename-dir-src"), loc("rename-dir-dst"),
                () -> callbackCalled[0] = true);

        Assertions.assertFalse(callbackCalled[0], "Callback should not be called when src exists");
        Assertions.assertFalse(fs.exists(loc("rename-dir-src")));
        Assertions.assertTrue(fs.exists(loc("rename-dir-dst")));
    }

    // ------------------------------------------------------------------
    // list
    // ------------------------------------------------------------------

    @Test
    @Order(5)
    void listFiles() throws IOException {
        writeContent("list-dir/file1.txt", "aaa".getBytes());
        writeContent("list-dir/file2.txt", "bbb".getBytes());
        fs.mkdirs(loc("list-dir/subdir"));

        List<FileEntry> files = fs.listFiles(loc("list-dir"));
        Assertions.assertTrue(files.size() >= 2,
                "Expected at least 2 files, got " + files.size());
    }

    @Test
    @Order(6)
    void listDirectories() throws IOException {
        fs.mkdirs(loc("dirs-test/sub1"));
        fs.mkdirs(loc("dirs-test/sub2"));
        writeContent("dirs-test/file.txt", "f".getBytes());

        Set<String> dirs = fs.listDirectories(loc("dirs-test"));
        Assertions.assertTrue(dirs.size() >= 2,
                "Expected at least 2 directories, got " + dirs.size());
    }

    // ------------------------------------------------------------------
    // IO round-trip
    // ------------------------------------------------------------------

    @Test
    @Order(7)
    void inputOutputRoundTrip() throws IOException {
        byte[] expected = "HDFS round-trip test 你好 🐘".getBytes();
        writeContent("roundtrip.bin", expected);
        byte[] actual = readAll("roundtrip.bin");
        Assertions.assertArrayEquals(expected, actual);
    }

    // ------------------------------------------------------------------
    // inputFile length
    // ------------------------------------------------------------------

    @Test
    @Order(8)
    void inputFileLength() throws IOException {
        byte[] data = new byte[3456];
        java.util.Arrays.fill(data, (byte) 'H');
        writeContent("length.bin", data);
        DorisInputFile inputFile = fs.newInputFile(loc("length.bin"));
        Assertions.assertEquals(3456L, inputFile.length());
    }
}
