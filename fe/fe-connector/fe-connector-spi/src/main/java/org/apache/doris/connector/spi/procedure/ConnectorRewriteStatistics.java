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

package org.apache.doris.connector.spi.procedure;

/**
 * What a distributed rewrite actually did, handed back to the connector so it can render the procedure's
 * result row ({@link ConnectorProcedureOps#buildRewriteResult}).
 *
 * <p>Three of the four — {@code dataFileCount}, {@code totalSizeBytes}, {@code deleteFileCount} — are straight
 * sums of the {@link ConnectorRewriteGroup}s the connector itself planned; the engine ran one
 * {@code INSERT-SELECT} per group, so counting the groups is part of orchestrating them, and no unit
 * conversion or reinterpretation happens on the way. The remaining one, {@code addedDataFileCount}, comes from
 * {@code RewriteCapableTransaction#getRewriteAddedDataFilesCount()} and is valid only after commit; it is the
 * one figure the engine cannot derive from planning. Note it is the SECOND constructor argument, not the last
 * — the constructor follows the result row's conventional column order, not this split. On the "nothing to
 * rewrite" path the engine reports all zeros without having opened a transaction.</p>
 *
 * <p>The field names mirror {@code ConnectorRewriteGroup}'s getters so the summation is obvious, and the
 * constructor order matches the order these values conventionally appear in a rewrite result row, so there is
 * one ordering to remember rather than two. All four are neutral words: how they are NAMED and TYPED in the
 * result set is the connector's decision, not this class's.</p>
 */
public final class ConnectorRewriteStatistics {

    private final int dataFileCount;
    private final int addedDataFileCount;
    private final long totalSizeBytes;
    private final int deleteFileCount;

    /**
     * @param dataFileCount      total data files the planned groups covered
     * @param addedDataFileCount data files the commit produced (post-commit only)
     * @param totalSizeBytes     total byte size of the data files the planned groups covered
     * @param deleteFileCount    total delete files attached to the planned groups
     */
    public ConnectorRewriteStatistics(int dataFileCount, int addedDataFileCount, long totalSizeBytes,
            int deleteFileCount) {
        this.dataFileCount = dataFileCount;
        this.addedDataFileCount = addedDataFileCount;
        this.totalSizeBytes = totalSizeBytes;
        this.deleteFileCount = deleteFileCount;
    }

    public int getDataFileCount() {
        return dataFileCount;
    }

    public int getAddedDataFileCount() {
        return addedDataFileCount;
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public int getDeleteFileCount() {
        return deleteFileCount;
    }

    @Override
    public String toString() {
        return "ConnectorRewriteStatistics{dataFileCount=" + dataFileCount
                + ", addedDataFileCount=" + addedDataFileCount
                + ", totalSizeBytes=" + totalSizeBytes
                + ", deleteFileCount=" + deleteFileCount + "}";
    }
}
