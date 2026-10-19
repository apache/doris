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

package org.apache.doris.datasource.split;

/**
 * One block of a file, described by its offset, its length and the hosts holding it.
 *
 * <p>This is the exact subset of {@code org.apache.hadoop.fs.BlockLocation} that
 * {@link FileSplitter} reads — {@code getOffset()}, {@code getLength()} and {@code getHosts()} —
 * reimplemented as a fe-core type so split planning carries no hadoop dependency. The hadoop
 * constructor's unused {@code names} argument is not carried over.
 *
 * <p>Null {@code hosts} is normalized to an empty array, matching hadoop: {@code new
 * BlockLocation(null, null, 0, len).getHosts()} returns {@code String[0]}, never null. Splitting a
 * file with no block information relies on that — it builds a single synthetic block with null
 * hosts, and the resulting splits must carry an empty host array rather than a null one.
 */
public class FileBlockLocation {

    private static final String[] EMPTY_HOSTS = new String[0];

    private final long offset;
    private final long length;
    private final String[] hosts;

    public FileBlockLocation(String[] hosts, long offset, long length) {
        this.hosts = hosts == null ? EMPTY_HOSTS : hosts;
        this.offset = offset;
        this.length = length;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public String[] getHosts() {
        return hosts;
    }
}
