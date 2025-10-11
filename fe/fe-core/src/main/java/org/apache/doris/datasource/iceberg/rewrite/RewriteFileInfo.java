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

package org.apache.doris.datasource.iceberg.rewrite;

/**
 * Information about files involved in a rewrite operation
 */
public class RewriteFileInfo {
    private final int filesToDeleteCount;
    private final int filesToAddCount;
    private final long filesToDeleteSize;
    private final long filesToAddSize;
    private final long totalRowsProcessed;
    private final int totalFilesProcessed;

    public RewriteFileInfo(int filesToDeleteCount, int filesToAddCount,
                          long filesToDeleteSize, long filesToAddSize,
                          long totalRowsProcessed, int totalFilesProcessed) {
        this.filesToDeleteCount = filesToDeleteCount;
        this.filesToAddCount = filesToAddCount;
        this.filesToDeleteSize = filesToDeleteSize;
        this.filesToAddSize = filesToAddSize;
        this.totalRowsProcessed = totalRowsProcessed;
        this.totalFilesProcessed = totalFilesProcessed;
    }

    public int getFilesToDeleteCount() {
        return filesToDeleteCount;
    }

    public int getFilesToAddCount() {
        return filesToAddCount;
    }

    public long getFilesToDeleteSize() {
        return filesToDeleteSize;
    }

    public long getFilesToAddSize() {
        return filesToAddSize;
    }

    public long getTotalRowsProcessed() {
        return totalRowsProcessed;
    }

    public int getTotalFilesProcessed() {
        return totalFilesProcessed;
    }

    @Override
    public String toString() {
        return "RewriteFileInfo{"
                + "filesToDeleteCount=" + filesToDeleteCount
                + ", filesToAddCount=" + filesToAddCount
                + ", filesToDeleteSize=" + filesToDeleteSize
                + ", filesToAddSize=" + filesToAddSize
                + ", totalRowsProcessed=" + totalRowsProcessed
                + ", totalFilesProcessed=" + totalFilesProcessed
                + '}';
    }
}
