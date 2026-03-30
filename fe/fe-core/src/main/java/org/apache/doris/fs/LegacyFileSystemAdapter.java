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

import org.apache.doris.backup.Status;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.ParsedPath;
import org.apache.doris.fs.remote.RemoteFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bridge adapter that implements the new {@link FileSystem} interface by delegating
 * to existing Status-based legacy methods.
 * <p>
 * Subclasses override the {@code legacy*()} methods (which have the old Status-based
 * signatures) and gain the new IOException-based interface for free.
 * <p>
 * Migration path:
 * <ol>
 *   <li>Existing classes that extend {@link org.apache.doris.fs.remote.RemoteFileSystem}
 *       (or its subclasses) should be gradually updated to extend this class instead.</li>
 *   <li>In Phase 3, when {@code RemoteFileSystem} is removed, all subclasses will directly
 *       extend this class and implement {@code legacy*()} methods.</li>
 * </ol>
 */
public abstract class LegacyFileSystemAdapter implements FileSystem {

    // ─────────────────────────── ABSTRACT LEGACY METHODS ───────────────────────────

    /** Checks file/directory existence. Returns {@link Status.ErrCode#NOT_FOUND} if absent. */
    protected abstract Status legacyExists(String remotePath);

    /** Deletes a file at {@code remotePath}. */
    protected abstract Status legacyDelete(String remotePath);

    /** Renames (moves) a file from {@code orig} to {@code dest}. */
    protected abstract Status legacyRename(String orig, String dest);

    /** Creates a directory at {@code remotePath} (including parents). */
    protected abstract Status legacyMakeDir(String remotePath);

    /**
     * Lists files under {@code remotePath}.
     * Results are appended to {@code result}.
     */
    protected abstract Status legacyListFiles(String remotePath, boolean recursive,
                                               List<RemoteFile> result);

    /**
     * Lists immediate child directories under {@code remotePath}.
     * Results are appended to {@code result}.
     * Default implementation throws UnsupportedOperationException.
     */
    protected Status legacyListDirectories(String remotePath, Set<String> result) {
        throw new UnsupportedOperationException(
                "listDirectories not supported by " + getClass().getSimpleName());
    }

    /**
     * Creates a new output file. Subclasses must implement.
     */
    protected abstract DorisOutputFile legacyNewOutputFile(ParsedPath path);

    /** Creates a new input file with optional length hint (-1 = unknown). */
    protected abstract DorisInputFile legacyNewInputFile(ParsedPath path, long length);

    // ─────────────────────────── NEW INTERFACE IMPLEMENTATIONS ───────────────────────────

    @Override
    public DorisInputFile newInputFile(Location location) {
        return legacyNewInputFile(new ParsedPath(location.toString()), -1L);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) {
        return legacyNewInputFile(new ParsedPath(location.toString()), length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) {
        return legacyNewOutputFile(new ParsedPath(location.toString()));
    }

    @Override
    public boolean exists(Location location) throws IOException {
        Status status = legacyExists(location.toString());
        if (status.ok()) {
            return true;
        }
        if (Status.ErrCode.NOT_FOUND.equals(status.getErrCode())) {
            return false;
        }
        throw new IOException("exists(" + location + ") failed: " + status.getErrMsg());
    }

    @Override
    public void deleteFile(Location location) throws IOException {
        Status status = legacyDelete(location.toString());
        if (!status.ok()) {
            throw new IOException("deleteFile(" + location + ") failed: " + status.getErrMsg());
        }
    }

    @Override
    public void renameFile(Location source, Location target) throws IOException {
        Status status = legacyRename(source.toString(), target.toString());
        if (!status.ok()) {
            throw new IOException("renameFile(" + source + " -> " + target + ") failed: "
                    + status.getErrMsg());
        }
    }

    @Override
    public void deleteDirectory(Location location) throws IOException {
        Status status = legacyDelete(location.toString());
        if (!status.ok()) {
            throw new IOException("deleteDirectory(" + location + ") failed: " + status.getErrMsg());
        }
    }

    @Override
    public void createDirectory(Location location) throws IOException {
        Status status = legacyMakeDir(location.toString());
        if (!status.ok()) {
            throw new IOException("createDirectory(" + location + ") failed: " + status.getErrMsg());
        }
    }

    @Override
    public void renameDirectory(Location source, Location target) throws IOException {
        Status status = legacyRename(source.toString(), target.toString());
        if (!status.ok()) {
            throw new IOException("renameDirectory(" + source + " -> " + target + ") failed: "
                    + status.getErrMsg());
        }
    }

    @Override
    public FileIterator listFiles(Location location, boolean recursive) throws IOException {
        List<RemoteFile> remoteFiles = new ArrayList<>();
        Status status = legacyListFiles(location.toString(), recursive, remoteFiles);
        if (!status.ok()) {
            throw new IOException("listFiles(" + location + ") failed: " + status.getErrMsg());
        }
        String base = location.toString();
        List<FileEntry> entries = remoteFiles.stream()
                .map(rf -> convertRemoteFile(rf, base))
                .collect(Collectors.toList());
        return FileIterator.ofList(entries);
    }

    @Override
    public Set<Location> listDirectories(Location location) throws IOException {
        Set<String> dirs = new HashSet<>();
        Status status = legacyListDirectories(location.toString(), dirs);
        if (!status.ok()) {
            throw new IOException("listDirectories(" + location + ") failed: " + status.getErrMsg());
        }
        Set<Location> result = new HashSet<>(dirs.size() * 2);
        for (String dir : dirs) {
            result.add(Location.of(dir));
        }
        return result;
    }

    // ─────────────────────────── HELPERS ───────────────────────────

    private static FileEntry convertRemoteFile(RemoteFile rf, String baseUri) {
        org.apache.hadoop.fs.Path hadoopPath = rf.getPath();
        Location loc = hadoopPath != null
                ? Location.of(hadoopPath.toString())
                : Location.of(baseUri.endsWith("/") ? baseUri + rf.getName()
                                                     : baseUri + "/" + rf.getName());

        FileEntry.Builder builder = FileEntry.builder(loc)
                .directory(rf.isDirectory())
                .length(rf.getSize())
                .blockSize(rf.getBlockSize())
                .modificationTime(rf.getModificationTime());

        if (rf.getBlockLocations() != null) {
            List<FileEntry.BlockInfo> blocks = new ArrayList<>();
            for (org.apache.hadoop.fs.BlockLocation bl : rf.getBlockLocations()) {
                try {
                    String[] hostsArr = bl.getHosts();
                    List<String> hosts = hostsArr != null
                            ? java.util.Arrays.asList(hostsArr)
                            : java.util.Collections.emptyList();
                    blocks.add(new FileEntry.BlockInfo(bl.getOffset(), bl.getLength(), hosts));
                } catch (IOException e) {
                    blocks.add(new FileEntry.BlockInfo(bl.getOffset(), bl.getLength(),
                            java.util.Collections.emptyList()));
                }
            }
            builder.blocks(blocks);
        }
        return builder.build();
    }
}
