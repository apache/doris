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

package org.apache.doris.fs.remote.dfs;

import org.apache.doris.fs.operations.HDFSFileOperations;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

class DFSFileSystemResource {

    private static final Logger LOG = LogManager.getLogger(DFSFileSystemResource.class);

    private final FileSystem fileSystem;
    private final HDFSFileOperations operations;
    private int referenceCount;
    private State state = State.OPEN;

    DFSFileSystemResource(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem");
        this.operations = new HDFSFileOperations(fileSystem);
    }

    synchronized ResourceLease acquire() throws IOException {
        if (state != State.OPEN) {
            throw new IOException("FileSystem is closing or closed.");
        }
        referenceCount++;
        return new ResourceLease(this);
    }

    synchronized void requestClose() {
        if (state == State.OPEN) {
            state = State.CLOSING;
        }
        closeIfIdle();
    }

    FileSystem fileSystem() {
        return fileSystem;
    }

    private synchronized void release() {
        Preconditions.checkState(referenceCount > 0, "FileSystem lease has been released more than once");
        referenceCount--;
        closeIfIdle();
    }

    private void closeIfIdle() {
        if (state != State.CLOSING || referenceCount != 0) {
            return;
        }
        state = State.CLOSED;
        try {
            fileSystem.close();
            LOG.info("Closed file system: {}", fileSystem.getUri());
        } catch (IOException e) {
            LOG.warn("Failed to close file system", e);
        }
    }

    private enum State {
        OPEN,
        CLOSING,
        CLOSED
    }

    static final class ResourceLease implements DFSFileSystem.FileSystemLease {
        private final DFSFileSystemResource resource;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ResourceLease(DFSFileSystemResource resource) {
            this.resource = resource;
        }

        @Override
        public FileSystem fileSystem() {
            return resource.fileSystem();
        }

        @Override
        public HDFSFileOperations operations() {
            return resource.operations;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            resource.release();
        }
    }
}
