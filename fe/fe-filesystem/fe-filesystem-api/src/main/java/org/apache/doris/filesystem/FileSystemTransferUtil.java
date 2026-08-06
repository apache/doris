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

package org.apache.doris.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility methods for spi.FileSystem that provide higher-level transfer operations.
 * All operations are built on the core spi.FileSystem primitives.
 */
public final class FileSystemTransferUtil {

    private FileSystemTransferUtil() {}

    /**
     * Downloads a remote file to a local path.
     *
     * @param expectedSize if > 0, verifies the downloaded byte count matches
     */
    public static void download(FileSystem fs, Location remote,
            Path localPath, long expectedSize) throws IOException {
        DorisInputFile input = fs.newInputFile(remote);
        try (InputStream in = input.newStream()) {
            long copied = Files.copy(in, localPath, StandardCopyOption.REPLACE_EXISTING);
            if (expectedSize > 0 && copied != expectedSize) {
                throw new IOException(String.format(
                        "Downloaded file size mismatch: expected %d, got %d for %s",
                        expectedSize, copied, remote));
            }
        }
    }

    /**
     * Uploads a local file to a remote location.
     */
    public static void upload(FileSystem fs, Path localPath, Location remote) throws IOException {
        DorisOutputFile output = fs.newOutputFile(remote);
        try (OutputStream out = output.createOrOverwrite();
                InputStream in = Files.newInputStream(localPath)) {
            copyStream(in, out);
        }
    }

    /**
     * Writes a string directly to a remote location without a local temp file.
     */
    public static void directUpload(FileSystem fs, String content,
            Location remote) throws IOException {
        DorisOutputFile output = fs.newOutputFile(remote);
        try (OutputStream out = output.createOrOverwrite()) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Lists all files under a location, optionally matching a glob pattern.
     * The glob pattern may contain {@code *} (any chars except '/') and {@code ?} (single char).
     *
     * @param locationStr remote path, may contain glob wildcards
     * @param recursive   whether to recurse into subdirectories
     */
    public static List<FileEntry> globList(FileSystem fs, String locationStr,
            boolean recursive) throws IOException {
        int wildcardIdx = indexOfFirstWildcard(locationStr);
        String basePath = wildcardIdx < 0
                ? locationStr
                : locationStr.substring(0, locationStr.lastIndexOf('/', wildcardIdx) + 1);
        Pattern pattern = wildcardIdx < 0 ? null : globToRegex(locationStr);

        List<FileEntry> result = new ArrayList<>();
        collectEntries(fs, Location.of(basePath), pattern, recursive, result);
        return result;
    }

    // ---- private helpers ----

    private static void copyStream(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[8192];
        int read;
        while ((read = in.read(buf)) != -1) {
            out.write(buf, 0, read);
        }
    }

    private static void collectEntries(FileSystem fs, Location base,
            Pattern pattern, boolean recursive,
            List<FileEntry> result) throws IOException {
        try (FileIterator iter = fs.list(base)) {
            while (iter.hasNext()) {
                FileEntry entry = iter.next();
                if (entry.isDirectory()) {
                    if (recursive) {
                        collectEntries(fs, entry.location(), pattern, true, result);
                    } else if (pattern != null && pattern.matcher(entry.location().uri()).matches()) {
                        // Include matching directories only when an explicit glob pattern is present.
                        // Without a pattern the caller expects a plain file listing.
                        result.add(entry);
                    }
                } else {
                    if (pattern == null || pattern.matcher(entry.location().uri()).matches()) {
                        result.add(entry);
                    }
                }
            }
        }
    }

    private static int indexOfFirstWildcard(String path) {
        int star = path.indexOf('*');
        int question = path.indexOf('?');
        if (star < 0) {
            return question;
        }
        if (question < 0) {
            return star;
        }
        return Math.min(star, question);
    }

    /** Converts a glob pattern (with * and ?) to a java.util.regex.Pattern. */
    public static Pattern globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    sb.append("[^/]*");
                    break;
                case '?':
                    sb.append("[^/]");
                    break;
                case '.':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                case '^':
                case '$':
                case '|':
                case '+':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append("$");
        return Pattern.compile(sb.toString());
    }
}
