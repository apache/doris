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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.util.Optional;

/**
 * Parameters for Iceberg data file rewrite operation
 */
public class RewriteParameters {
    private final long targetFileSizeBytes;
    private final long minFileSizeBytes;
    private final long maxFileSizeBytes;
    private final int minInputFiles;
    private final boolean rewriteAll;
    private final long maxFileGroupSizeBytes;
    private final int deleteFileThreshold;
    private final double deleteRatioThreshold;
    private final long outputSpecId;

    private final Optional<PartitionNamesInfo> partitionFilter;
    private final Optional<Expression> whereCondition;

    public RewriteParameters(long targetFileSizeBytes,
            long minFileSizeBytes,
            long maxFileSizeBytes,
            int minInputFiles,
            boolean rewriteAll,
            long maxFileGroupSizeBytes,
            int deleteFileThreshold,
            double deleteRatioThreshold,
            long outputSpecId,
            Optional<PartitionNamesInfo> partitionFilter,
            Optional<Expression> whereCondition) {
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.minFileSizeBytes = minFileSizeBytes;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.minInputFiles = minInputFiles;
        this.rewriteAll = rewriteAll;
        this.maxFileGroupSizeBytes = maxFileGroupSizeBytes;
        this.deleteFileThreshold = deleteFileThreshold;
        this.deleteRatioThreshold = deleteRatioThreshold;
        this.outputSpecId = outputSpecId;
        this.partitionFilter = partitionFilter;
        this.whereCondition = whereCondition;
    }

    // Getters
    public long getTargetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    public long getMinFileSizeBytes() {
        return minFileSizeBytes;
    }

    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    public int getMinInputFiles() {
        return minInputFiles;
    }

    public boolean isRewriteAll() {
        return rewriteAll;
    }

    public long getMaxFileGroupSizeBytes() {
        return maxFileGroupSizeBytes;
    }

    public int getDeleteFileThreshold() {
        return deleteFileThreshold;
    }

    public double getDeleteRatioThreshold() {
        return deleteRatioThreshold;
    }

    public long getOutputSpecId() {
        return outputSpecId;
    }

    public boolean hasPartitionFilter() {
        return partitionFilter.isPresent();
    }

    public Optional<PartitionNamesInfo> getPartitionFilter() {
        return partitionFilter;
    }

    public boolean hasWhereCondition() {
        return whereCondition.isPresent();
    }

    public Optional<Expression> getWhereCondition() {
        return whereCondition;
    }

    @Override
    public String toString() {
        return "RewriteParameters{" +
                "targetFileSizeBytes=" + targetFileSizeBytes +
                ", minFileSizeBytes=" + minFileSizeBytes +
                ", maxFileSizeBytes=" + maxFileSizeBytes +
                ", minInputFiles=" + minInputFiles +
                ", rewriteAll=" + rewriteAll +
                ", maxFileGroupSizeBytes=" + maxFileGroupSizeBytes +
                ", deleteFileThreshold=" + deleteFileThreshold +
                ", deleteRatioThreshold=" + deleteRatioThreshold +
                ", outputSpecId=" + outputSpecId +
                ", hasPartitionFilter=" + hasPartitionFilter() +
                ", hasWhereCondition=" + hasWhereCondition() +
                '}';
    }
}