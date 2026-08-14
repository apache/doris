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

package org.apache.doris.datasource.lance;

/** Immutable row-count metadata for one Lance fragment in a fixed dataset snapshot. */
public final class LanceFragmentInfo {
    private final long id;
    private final long rowCount;
    private final long physicalRows;

    public LanceFragmentInfo(long id, long rowCount, long physicalRows) {
        this.id = id;
        this.rowCount = rowCount;
        this.physicalRows = physicalRows;
    }

    /**
     * Returns the fragment id stored by Lance as an unsigned 32-bit value.
     *
     * <p>Doris represents it as a {@code long} so ids with the high bit set do not become
     * negative while crossing Java and Thrift boundaries.
     */
    public long getId() {
        return id;
    }

    /** Returns the logical row count after deletion vectors have been applied. */
    public long getRowCount() {
        return rowCount;
    }

    /**
     * Returns the number of physical rows stored before deletions.
     *
     * <p>The BE legacy reader reads and merges physical batches before applying the deletion
     * vector, so split scheduling uses this value rather than {@link #getRowCount()}.
     */
    public long getPhysicalRows() {
        return physicalRows;
    }
}
