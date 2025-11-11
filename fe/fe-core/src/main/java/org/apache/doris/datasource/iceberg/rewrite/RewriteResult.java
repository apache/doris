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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Result of Iceberg data file rewrite operation
 */
public class RewriteResult {
    private int rewrittenDataFilesCount;
    private int addedDataFilesCount;
    private long rewrittenBytesCount;
    private int removedDeleteFilesCount;

    public RewriteResult() {
        this(0, 0, 0, 0);
    }

    public RewriteResult(int rewrittenDataFilesCount,
            int addedDataFilesCount,
            long rewrittenBytesCount,
            int removedDeleteFilesCount) {
        this.rewrittenDataFilesCount = rewrittenDataFilesCount;
        this.addedDataFilesCount = addedDataFilesCount;
        this.rewrittenBytesCount = rewrittenBytesCount;
        this.removedDeleteFilesCount = removedDeleteFilesCount;
    }

    /**
     * Merge another result into this one
     */
    public void merge(RewriteResult other) {
        this.rewrittenDataFilesCount += other.rewrittenDataFilesCount;
        this.addedDataFilesCount += other.addedDataFilesCount;
        this.rewrittenBytesCount += other.rewrittenBytesCount;
        this.removedDeleteFilesCount += other.removedDeleteFilesCount;
    }

    /**
     * Convert to string list for display
     */
    public List<String> toStringList() {
        return Lists.newArrayList(
                String.valueOf(rewrittenDataFilesCount),
                String.valueOf(addedDataFilesCount),
                String.valueOf(rewrittenBytesCount),
                String.valueOf(removedDeleteFilesCount));
    }

    // Getters
    public int getRewrittenDataFilesCount() {
        return rewrittenDataFilesCount;
    }

    public int getAddedDataFilesCount() {
        return addedDataFilesCount;
    }

    public long getRewrittenBytesCount() {
        return rewrittenBytesCount;
    }

    public int getRemovedDeleteFilesCount() {
        return removedDeleteFilesCount;
    }

    // Setters
    public void setRewrittenDataFilesCount(int rewrittenDataFilesCount) {
        this.rewrittenDataFilesCount = rewrittenDataFilesCount;
    }

    public void setAddedDataFilesCount(int addedDataFilesCount) {
        this.addedDataFilesCount = addedDataFilesCount;
    }

    public void setRewrittenBytesCount(long rewrittenBytesCount) {
        this.rewrittenBytesCount = rewrittenBytesCount;
    }

    public void setRemovedDeleteFilesCount(int removedDeleteFilesCount) {
        this.removedDeleteFilesCount = removedDeleteFilesCount;
    }

    @Override
    public String toString() {
        return "RewriteResult{"
                + "rewrittenDataFilesCount=" + rewrittenDataFilesCount
                + ", addedDataFilesCount=" + addedDataFilesCount
                + ", rewrittenBytesCount=" + rewrittenBytesCount
                + ", removedDeleteFilesCount=" + removedDeleteFilesCount
                + '}';
    }
}
