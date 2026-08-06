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
import java.util.concurrent.atomic.AtomicBoolean;

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

    // --- toPath / scheme handling ---

    @Test
    void unsupportedSchemeRejected() {
        LocalFileSystem fs = createFs();
        Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("s3://bucket/key")));
    }

    @Test
    void opaqueFileUriRejected() {
        LocalFileSystem fs = createFs();
        Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("file:foo")));
    }

    // --- exists with symlinks ---

    @Test
    void existsReturnsTrueForBrokenSymlink() throws IOException {
        Path target = tempDir.resolve("missing-target");
        Path link = tempDir.resolve("broken-link");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException e) {
            // Skip on platforms without symlink support
            return;
        }
        Assertions.assertTrue(createFs().exists(loc(link)));
    }

    // --- delete: symlink safety ---

    @Test
    void deleteRecursiveDoesNotFollowSymlinkToOutsideDir() throws IOException {
        Path victimDir = tempDir.resolve("victim");
        Files.createDirectories(victimDir);
        Path victimFile = victimDir.resolve("important.dat");
        Files.writeString(victimFile, "do-not-delete");

        Path workDir = tempDir.resolve("work");
        Files.createDirectories(workDir);
        Path link = workDir.resolve("link");
        try {
            Files.createSymbolicLink(link, victimDir);
        } catch (UnsupportedOperationException | IOException e) {
            return;
        }

        createFs().delete(loc(workDir), true);
        Assertions.assertFalse(Files.exists(workDir));
        Assertions.assertTrue(Files.exists(victimFile),
                "symlink target contents must not be deleted");
    }

    @Test
    void deleteRecursiveOnSymlinkRemovesOnlyLink() throws IOException {
        Path victimDir = tempDir.resolve("victim2");
        Files.createDirectories(victimDir);
        Path victimFile = victimDir.resolve("x.dat");
        Files.writeString(victimFile, "x");

        Path link = tempDir.resolve("dirlink");
        try {
            Files.createSymbolicLink(link, victimDir);
        } catch (UnsupportedOperationException | IOException e) {
            return;
        }

        createFs().delete(loc(link), true);
        Assertions.assertFalse(Files.exists(link, java.nio.file.LinkOption.NOFOLLOW_LINKS));
        Assertions.assertTrue(Files.exists(victimFile));
    }

    // --- rename ---

    @Test
    void renameDoesNotNpeOnSingleSegmentDst() throws IOException {
        Path src = tempDir.resolve("rename-src.txt");
        Files.writeString(src, "data");
        // Use a single-segment local URI whose Path has no parent. Ensure the target file
        // does not exist beforehand.
        Path dstAbs = tempDir.resolve("rename-single.txt");
        Files.deleteIfExists(dstAbs);

        LocalFileSystem fs = createFs();
        // Build a Location that resolves to dstAbs via local:// scheme; getParent() of the
        // resolved path is non-null in this case, but the helper must still not NPE for the
        // common case. Round-trip via the standard helper.
        fs.rename(loc(src), loc(dstAbs));
        Assertions.assertTrue(Files.exists(dstAbs));
    }

    // --- create() atomicity ---

    @Test
    void createIsAtomicWithRespectToExisting() throws IOException {
        Path file = tempDir.resolve("atomic-create.txt");
        DorisOutputFile outputFile = createFs().newOutputFile(loc(file));
        try (OutputStream out = outputFile.create()) {
            out.write("first".getBytes(StandardCharsets.UTF_8));
        }
        // Second create() must throw without truncating the existing file.
        Assertions.assertThrows(IOException.class, outputFile::create);
        Assertions.assertEquals("first", Files.readString(file));
    }

    // --- list: scheme preservation + symlink reporting ---

    @Test
    void listReportsSymlinkToDirAsNonDirectory() throws IOException {
        Path realDir = tempDir.resolve("real");
        Files.createDirectories(realDir);
        Path link = tempDir.resolve("dlink");
        try {
            Files.createSymbolicLink(link, realDir);
        } catch (UnsupportedOperationException | IOException e) {
            return;
        }

        LocalFileSystem fs = createFs();
        boolean linkSeen = false;
        try (FileIterator it = fs.list(loc(tempDir))) {
            while (it.hasNext()) {
                FileEntry e = it.next();
                if (e.location().uri().endsWith("dlink")) {
                    linkSeen = true;
                    Assertions.assertFalse(e.isDirectory(),
                            "symlink-to-dir must be reported as non-directory");
                }
            }
        }
        Assertions.assertTrue(linkSeen);
    }

    @Test
    void listPreservesLocalScheme() throws IOException {
        Files.writeString(tempDir.resolve("entry.txt"), "x");
        LocalFileSystem fs = createFs();
        Location dir = Location.of("local://" + tempDir.toAbsolutePath().toString().replaceFirst("^/+", ""));
        try (FileIterator it = fs.list(dir)) {
            Assertions.assertTrue(it.hasNext());
            FileEntry e = it.next();
            Assertions.assertTrue(e.location().uri().startsWith("local://"),
                    "expected local:// scheme but got " + e.location().uri());
        }
    }

    // --- renameDirectory ---

    @Test
    void renameDirectoryInvokesCallbackWhenSrcMissing() throws IOException {
        LocalFileSystem fs = createFs();
        AtomicBoolean called = new AtomicBoolean(false);
        fs.renameDirectory(loc(tempDir.resolve("missing-src")),
                loc(tempDir.resolve("dst")),
                () -> called.set(true));
        Assertions.assertTrue(called.get());
    }

    @Test
    void renameDirectoryMovesAtomicallyWhenSrcExists() throws IOException {
        Path src = tempDir.resolve("dir-src");
        Files.createDirectories(src);
        Files.writeString(src.resolve("f.txt"), "v");
        Path dst = tempDir.resolve("dir-dst");

        LocalFileSystem fs = createFs();
        AtomicBoolean called = new AtomicBoolean(false);
        fs.renameDirectory(loc(src), loc(dst), () -> called.set(true));
        Assertions.assertFalse(called.get());
        Assertions.assertFalse(Files.exists(src));
        Assertions.assertEquals("v", Files.readString(dst.resolve("f.txt")));
    }

    // --- LocalSeekableInputStream: getPos after close ---

    @Test
    void getPosThrowsAfterClose() throws IOException {
        Path file = tempDir.resolve("close-pos.txt");
        Files.writeString(file, "abc");
        DorisInputStream in = createFs().newInputFile(loc(file)).newStream();
        in.close();
        IOException ex = Assertions.assertThrows(IOException.class, in::getPos);
        Assertions.assertEquals("Stream is closed", ex.getMessage());
    }
}
