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
import java.util.List;
import java.util.Set;

/**
 * Adapts any old-style {@link LegacyFileSystemApi} implementation to the new
 * {@link FileSystem} interface via {@link LegacyFileSystemAdapter}.
 * <p>
 * This avoids modifying existing class hierarchies in Phase 1.
 * In Phase 3, when RemoteFileSystem is removed, direct subclassing of
 * LegacyFileSystemAdapter will replace this adapter.
 */
public final class LegacyToNewFsAdapter extends LegacyFileSystemAdapter {

    private final LegacyFileSystemApi delegate;

    public LegacyToNewFsAdapter(LegacyFileSystemApi delegate) {
        this.delegate = delegate;
    }

    @Override
    protected Status legacyExists(String remotePath) {
        return delegate.exists(remotePath);
    }

    @Override
    protected Status legacyDelete(String remotePath) {
        return delegate.delete(remotePath);
    }

    @Override
    protected Status legacyRename(String orig, String dest) {
        return delegate.rename(orig, dest);
    }

    @Override
    protected Status legacyMakeDir(String remotePath) {
        return delegate.makeDir(remotePath);
    }

    @Override
    protected Status legacyListFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        return delegate.listFiles(remotePath, recursive, result);
    }

    @Override
    protected Status legacyListDirectories(String remotePath, Set<String> result) {
        return delegate.listDirectories(remotePath, result);
    }

    @Override
    protected DorisOutputFile legacyNewOutputFile(ParsedPath path) {
        return delegate.newOutputFile(path);
    }

    @Override
    protected DorisInputFile legacyNewInputFile(ParsedPath path, long length) {
        return delegate.newInputFile(path, length);
    }

    @Override
    public void close() throws IOException {
        if (delegate instanceof java.io.Closeable) {
            ((java.io.Closeable) delegate).close();
        }
    }
}
