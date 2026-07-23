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

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.HadoopAuthenticator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * {@link FileIterator} backed by a Hadoop {@link RemoteIterator}{@code <FileStatus>} for lazy,
 * streaming directory listing. Each {@code hasNext()} / {@code next()} call is executed inside the
 * provided {@link HadoopAuthenticator} context so that Kerberos tickets remain valid throughout.
 */
class HdfsFileIterator implements FileIterator {

    private final RemoteIterator<FileStatus> delegate;
    private final HadoopAuthenticator authenticator;
    private volatile boolean closed;

    HdfsFileIterator(RemoteIterator<FileStatus> delegate, HadoopAuthenticator authenticator) {
        this.delegate = delegate;
        this.authenticator = authenticator;
    }

    @Override
    public boolean hasNext() throws IOException {
        return authenticator.doAs(delegate::hasNext);
    }

    @Override
    public FileEntry next() throws IOException {
        FileStatus status = authenticator.doAs(delegate::next);
        Location loc = Location.of(status.getPath().toString());
        return new FileEntry(loc, status.getLen(), status.isDirectory(),
                status.getModificationTime(), null);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // RemoteIterator itself has no close() method in the public Hadoop API, but some
        // concrete implementations (e.g. DistributedFileSystem's listing iterator) also
        // implement Closeable to release server-side cursors. Best-effort propagation.
        if (delegate instanceof Closeable) {
            ((Closeable) delegate).close();
        }
    }
}
