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

package org.apache.doris.filesystem.local;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class LocalFileSystemTest {

    @TempDir
    Path tempDir;

    private LocalFileSystem createFs() {
        return new LocalFileSystem(Map.of());
    }

    private Location loc(Path path) {
        return Location.of(path.toUri().toString());
    }

    // --- exists ---

    @Test
    void existsReturnsTrueForExistingFile() throws IOException {
        Path file = tempDir.resolve("test.txt");
        Files.writeString(file, "hello");
        Assertions.assertTrue(createFs().exists(loc(file)));
    }

    @Test
    void existsReturnsFalseForNonExistent() throws IOException {
        Assertions.assertFalse(createFs().exists(loc(tempDir.resolve("nonexistent"))));
    }

    // --- mkdirs ---

    @Test
    void mkdirsCreatesMultipleLevels() throws IOException {
        LocalFileSystem fs = createFs();
        Path deep = tempDir.resolve("a/b/c");
        fs.mkdirs(loc(deep));
        Assertions.assertTrue(Files.isDirectory(deep));
    }

    // --- delete ---

    @Test
    void deleteFileRemovesIt() throws IOException {
        Path file = tempDir.resolve("todelete.txt");
        Files.writeString(file, "content");
        LocalFileSystem fs = createFs();
        fs.delete(loc(file), false);
        Assertions.assertFalse(Files.exists(file));
    }

    @Test
    void deleteRecursiveRemovesDirectoryTree() throws IOException {
        Path dir = tempDir.resolve("mydir");
        Files.createDirectories(dir.resolve("sub"));
        Files.writeString(dir.resolve("sub/file.txt"), "data");
        Files.writeString(dir.resolve("root.txt"), "data");

        LocalFileSystem fs = createFs();
        fs.delete(loc(dir), true);
        Assertions.assertFalse(Files.exists(dir));
    }

    // --- rename ---

    @Test
    void renameMovesFile() throws IOException {
        Path src = tempDir.resolve("src.txt");
        Path dst = tempDir.resolve("dst.txt");
        Files.writeString(src, "hello");

        LocalFileSystem fs = createFs();
        fs.rename(loc(src), loc(dst));
        Assertions.assertFalse(Files.exists(src));
        Assertions.assertTrue(Files.exists(dst));
        Assertions.assertEquals("hello", Files.readString(dst));
    }

    // --- list ---

    @Test
    void listReturnsCorrectEntries() throws IOException {
        Files.writeString(tempDir.resolve("a.txt"), "aaa");
        Files.writeString(tempDir.resolve("b.txt"), "bb");
        Files.createDirectory(tempDir.resolve("subdir"));

        LocalFileSystem fs = createFs();
        List<FileEntry> entries = new ArrayList<>();
        try (FileIterator it = fs.list(loc(tempDir))) {
            while (it.hasNext()) {
                entries.add(it.next());
            }
        }

        Assertions.assertEquals(3, entries.size());
    }

    // --- newInputFile + newOutputFile round-trip ---

    @Test
    void writeAndReadRoundTrip() throws IOException {
        LocalFileSystem fs = createFs();
        Location fileLoc = loc(tempDir.resolve("roundtrip.txt"));
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        // Write
        DorisOutputFile outputFile = fs.newOutputFile(fileLoc);
        try (OutputStream out = outputFile.createOrOverwrite()) {
            out.write(data);
        }

        // Read
        DorisInputFile inputFile = fs.newInputFile(fileLoc);
        byte[] readBack;
        try (DorisInputStream in = inputFile.newStream()) {
            readBack = in.readAllBytes();
        }

        Assertions.assertArrayEquals(data, readBack);
    }

    @Test
    void inputFileLengthReturnsActualSize() throws IOException {
        Path file = tempDir.resolve("sized.txt");
        byte[] data = "12345".getBytes(StandardCharsets.UTF_8);
        Files.write(file, data);

        DorisInputFile inputFile = createFs().newInputFile(loc(file));
        Assertions.assertEquals(data.length, inputFile.length());
    }

    @Test
    void inputFileExistsReturnsCorrectResult() throws IOException {
        LocalFileSystem fs = createFs();
        Path file = tempDir.resolve("exist-check.txt");
        Files.writeString(file, "x");

        DorisInputFile inputFile = fs.newInputFile(loc(file));
        Assertions.assertTrue(inputFile.exists());

        Files.delete(file);
        Assertions.assertFalse(inputFile.exists());
    }

    // --- outputFile create vs createOrOverwrite ---

    @Test
    void outputFileCreateThrowsIfFileExists() throws IOException {
        Path file = tempDir.resolve("existing.txt");
        Files.writeString(file, "x");
        DorisOutputFile outputFile = createFs().newOutputFile(loc(file));
        Assertions.assertThrows(IOException.class, outputFile::create);
    }

    @Test
    void outputFileCreateOrOverwriteReplacesContent() throws IOException {
        Path file = tempDir.resolve("overwrite.txt");
        Files.writeString(file, "original");

        DorisOutputFile outputFile = createFs().newOutputFile(loc(file));
        try (OutputStream out = outputFile.createOrOverwrite()) {
            out.write("replaced".getBytes(StandardCharsets.UTF_8));
        }
        Assertions.assertEquals("replaced", Files.readString(file));
    }

    // --- seekable input stream ---

    @Test
    void seekableStreamSupportsSeekAndGetPos() throws IOException {
        Path file = tempDir.resolve("seekable.txt");
        Files.writeString(file, "ABCDEFGHIJ");

        DorisInputFile inputFile = createFs().newInputFile(loc(file));
        try (DorisInputStream in = inputFile.newStream()) {
            Assertions.assertEquals(0, in.getPos());
            Assertions.assertEquals('A', (char) in.read());
            Assertions.assertEquals(1, in.getPos());

            in.seek(5);
            Assertions.assertEquals(5, in.getPos());
            Assertions.assertEquals('F', (char) in.read());
        }
    }
}
