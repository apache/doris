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

package org.apache.doris.fs;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemTransferUtil;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.local.LocalFileSystemProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FileSystemTransferUtilTest {

    private Path tempDir;
    private FileSystem fs;

    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("doris-fs-test-");
        Map<String, String> props = new HashMap<>();
        props.put("uri", "file://" + tempDir);
        fs = new LocalFileSystemProvider().create(props);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        // best-effort cleanup
                    }
                });
            }
        }
    }

    private static void writeFile(Path path, String content) throws IOException {
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    }

    private static String readFile(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    // ---- download ----

    @Test
    public void testDownloadSuccess() throws IOException {
        String content = "hello download";
        Path remoteFile = tempDir.resolve("remote.txt");
        writeFile(remoteFile, content);

        Path localOut = tempDir.resolve("local_out.txt");
        FileSystemTransferUtil.download(fs, Location.of(remoteFile.toUri().toString()),
                localOut, content.length());

        Assertions.assertEquals(content, readFile(localOut));
    }

    @Test
    public void testDownloadSizeMismatchThrows() throws IOException {
        Path remoteFile = tempDir.resolve("data.txt");
        writeFile(remoteFile, "abc");

        Path localOut = tempDir.resolve("out.txt");
        IOException ex = Assertions.assertThrows(IOException.class, () ->
                FileSystemTransferUtil.download(fs, Location.of(remoteFile.toUri().toString()),
                        localOut, 999L));
        Assertions.assertTrue(ex.getMessage().contains("size mismatch"));
    }

    @Test
    public void testDownloadSkipsSizeCheckWhenNegative() throws IOException {
        Path remoteFile = tempDir.resolve("no_check.txt");
        writeFile(remoteFile, "data");

        Path localOut = tempDir.resolve("local_no_check.txt");
        FileSystemTransferUtil.download(fs, Location.of(remoteFile.toUri().toString()),
                localOut, -1L);

        Assertions.assertEquals("data", readFile(localOut));
    }

    // ---- upload ----

    @Test
    public void testUpload() throws IOException {
        Path localSrc = tempDir.resolve("local_src.txt");
        writeFile(localSrc, "upload content");

        Path remoteDest = tempDir.resolve("sub").resolve("uploaded.txt");
        Files.createDirectories(remoteDest.getParent());

        FileSystemTransferUtil.upload(fs, localSrc, Location.of(remoteDest.toUri().toString()));

        Assertions.assertEquals("upload content", readFile(remoteDest));
    }

    // ---- directUpload ----

    @Test
    public void testDirectUpload() throws IOException {
        String content = "direct content\nline2";
        Path dest = tempDir.resolve("direct.txt");

        FileSystemTransferUtil.directUpload(fs, content, Location.of(dest.toUri().toString()));

        Assertions.assertEquals(content, readFile(dest));
    }

    // ---- globList ----

    @Test
    public void testGlobListNoWildcard() throws IOException {
        writeFile(tempDir.resolve("a.txt"), "a");
        writeFile(tempDir.resolve("b.txt"), "b");
        Files.createDirectory(tempDir.resolve("subdir"));
        writeFile(tempDir.resolve("subdir").resolve("c.txt"), "c");

        List<FileEntry> entries = FileSystemTransferUtil.globList(
                fs, tempDir.toUri().toString(), false);

        Assertions.assertEquals(2, entries.size());
    }

    @Test
    public void testGlobListRecursive() throws IOException {
        writeFile(tempDir.resolve("root.parquet"), "r");
        Path sub = Files.createDirectory(tempDir.resolve("sub"));
        writeFile(sub.resolve("nested.parquet"), "n");

        List<FileEntry> entries = FileSystemTransferUtil.globList(
                fs, tempDir.toUri().toString(), true);

        Assertions.assertEquals(2, entries.size());
    }

    @Test
    public void testGlobListWithStarWildcard() throws IOException {
        writeFile(tempDir.resolve("part-0001.snappy.parquet"), "p1");
        writeFile(tempDir.resolve("part-0002.snappy.parquet"), "p2");
        writeFile(tempDir.resolve("manifest.json"), "m");

        String pattern = tempDir.toUri().toString() + "*.parquet";
        List<FileEntry> entries = FileSystemTransferUtil.globList(fs, pattern, false);

        Assertions.assertEquals(2, entries.size());
        entries.forEach(e -> Assertions.assertTrue(e.location().uri().endsWith(".parquet")));
    }

    @Test
    public void testGlobListWithQuestionMark() throws IOException {
        writeFile(tempDir.resolve("file1.txt"), "1");
        writeFile(tempDir.resolve("file2.txt"), "2");
        writeFile(tempDir.resolve("file10.txt"), "10");

        String pattern = tempDir.toUri().toString() + "file?.txt";
        List<FileEntry> entries = FileSystemTransferUtil.globList(fs, pattern, false);

        Assertions.assertEquals(2, entries.size());
    }

    // ---- globToRegex ----

    @Test
    public void testGlobToRegexStar() {
        Pattern p = FileSystemTransferUtil.globToRegex("s3://bucket/prefix/*.parquet");
        Assertions.assertTrue(p.matcher("s3://bucket/prefix/file.parquet").matches());
        Assertions.assertFalse(p.matcher("s3://bucket/prefix/sub/file.parquet").matches());
    }

    @Test
    public void testGlobToRegexQuestion() {
        Pattern p = FileSystemTransferUtil.globToRegex("/data/part-?.csv");
        Assertions.assertTrue(p.matcher("/data/part-1.csv").matches());
        Assertions.assertFalse(p.matcher("/data/part-10.csv").matches());
    }

    @Test
    public void testGlobToRegexDotEscaped() {
        Pattern p = FileSystemTransferUtil.globToRegex("s3://bucket/file.parquet");
        Assertions.assertTrue(p.matcher("s3://bucket/file.parquet").matches());
        Assertions.assertFalse(p.matcher("s3://bucket/fileXparquet").matches());
    }
}
