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

import org.apache.doris.filesystem.spi.FileEntry;
import org.apache.doris.filesystem.spi.FileIterator;
import org.apache.doris.filesystem.spi.Location;

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

/**
 * {@link FileIterator} backed by a Hadoop {@link FileStatus} array from a single listStatus call.
 */
class HdfsFileIterator implements FileIterator {

    private final FileStatus[] statuses;
    private int index = 0;

    HdfsFileIterator(FileStatus[] statuses) {
        this.statuses = statuses;
    }

    @Override
    public boolean hasNext() throws IOException {
        return index < statuses.length;
    }

    @Override
    public FileEntry next() throws IOException {
        FileStatus status = statuses[index++];
        Location loc = Location.of(status.getPath().toString());
        return new FileEntry(loc, status.getLen(), status.isDirectory(),
                status.getModificationTime(), null);
    }

    @Override
    public void close() throws IOException {
        // no-op: statuses array is already fully loaded
    }
}
