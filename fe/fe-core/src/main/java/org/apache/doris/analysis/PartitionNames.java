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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * To represent following stmt:
 *      PARTITION p1
 *      TEMPORARY PARTITION p1
 *      PARTITION (p1, p2)
 *      TEMPORARY PARTITION (p1, p2)
 *      PARTITIONS (p1, p2)
 *      TEMPORARY PARTITIONS (p1, p2)
 */
public class PartitionNames implements ParseNode, Writable {

    @SerializedName(value = "partitionNames")
    private final List<String> partitionNames;
    // true if these partitions are temp partitions
    @SerializedName(value = "isTemp")
    private final boolean isTemp;
    private final boolean allPartitions;
    private final long count;
    // Default partition count to collect statistic for external table.
    private static final long DEFAULT_PARTITION_COUNT = 100;

    public PartitionNames(boolean isTemp, List<String> partitionNames) {
        this.partitionNames = partitionNames;
        this.isTemp = isTemp;
        this.allPartitions = false;
        this.count = 0;
    }

    public PartitionNames(PartitionNames other) {
        this.partitionNames = Lists.newArrayList(other.partitionNames);
        this.isTemp = other.isTemp;
        this.allPartitions = other.allPartitions;
        this.count = 0;
    }

    public PartitionNames(boolean allPartitions) {
        this.partitionNames = null;
        this.isTemp = false;
        this.allPartitions = allPartitions;
        this.count = 0;
    }

    public PartitionNames(long partitionCount) {
        this.partitionNames = null;
        this.isTemp = false;
        this.allPartitions = false;
        this.count = partitionCount;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public boolean isAllPartitions() {
        return allPartitions;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (allPartitions && count > 0) {
            throw new AnalysisException("All partition and partition count couldn't be set at the same time.");
        }
        if (allPartitions || count > 0) {
            return;
        }
        if (partitionNames == null || partitionNames.isEmpty()) {
            throw new AnalysisException("No partition specified in partition lists");
        }
        // check if partition name is not empty string
        if (partitionNames.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new AnalysisException("there are empty partition name");
        }
    }

    @Override
    public String toSql() {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (isTemp) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITIONS (");
        sb.append(Joiner.on(", ").join(partitionNames));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionNames read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionNames.class);
    }
}
