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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

public class DFSFileSystemResourceTest {

    @Test
    public void testRequestCloseWaitsForActiveLease() throws Exception {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);

        DFSFileSystem.FileSystemLease lease = resource.acquire();
        resource.requestClose();
        Assert.assertEquals(0, fileSystem.getCloseCount());

        lease.close();
        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testRequestCloseWaitsForAllActiveLeases() throws Exception {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);

        DFSFileSystem.FileSystemLease firstLease = resource.acquire();
        DFSFileSystem.FileSystemLease secondLease = resource.acquire();
        resource.requestClose();
        Assert.assertEquals(0, fileSystem.getCloseCount());

        firstLease.close();
        Assert.assertEquals(0, fileSystem.getCloseCount());

        secondLease.close();
        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testRequestCloseWithoutLeaseClosesImmediately() {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);

        resource.requestClose();

        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testRequestCloseIsIdempotent() {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);

        resource.requestClose();
        resource.requestClose();

        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testLeaseCloseIsIdempotent() throws Exception {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);
        DFSFileSystem.FileSystemLease lease = resource.acquire();

        resource.requestClose();
        lease.close();
        lease.close();

        Assert.assertEquals(1, fileSystem.getCloseCount());
    }

    @Test
    public void testAcquireAfterRequestCloseThrows() throws Exception {
        DFSFileSystemResource resource = new DFSFileSystemResource(new CountingFileSystem());

        resource.requestClose();
        Assert.assertThrows(IOException.class, resource::acquire);
    }

    @Test
    public void testAcquireAfterLastLeaseReleaseThrows() throws Exception {
        CountingFileSystem fileSystem = new CountingFileSystem();
        DFSFileSystemResource resource = new DFSFileSystemResource(fileSystem);
        DFSFileSystem.FileSystemLease lease = resource.acquire();

        resource.requestClose();
        lease.close();

        Assert.assertEquals(1, fileSystem.getCloseCount());
        Assert.assertThrows(IOException.class, resource::acquire);
    }

    private static class CountingFileSystem extends FileSystem {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public URI getUri() {
            return URI.create("hdfs://counting");
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
                short replication, long blockSize, Progressable progress) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path path) {
        }

        @Override
        public Path getWorkingDirectory() {
            return new Path("/");
        }

        @Override
        public boolean mkdirs(Path path, FsPermission permission) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            closeCount.incrementAndGet();
        }

        int getCloseCount() {
            return closeCount.get();
        }
    }
}
