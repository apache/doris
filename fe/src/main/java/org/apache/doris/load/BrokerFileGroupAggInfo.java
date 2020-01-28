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

package org.apache.doris.load;

import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This class is mainly used to aggregate information of multiple DataDescriptors.
 * When the table name and specified partitions in the two DataDescriptors are same,
 * the BrokerFileGroup information corresponding to the two DataDescriptors will be aggregated together.
 * eg.
 * 
 * DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file1")
 * INTO TABLE `tbl1`
 * PARTITION (p1, p2)
 * 
 * and
 * 
 * DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file2")
 * INTO TABLE `tbl1`
 * PARTITION (p1, p2)
 *
 * will be aggregated together, because they have same table name and specified partitions
 * =>
 * FileGroupAggKey(tbl1, [p1, p2]) => List(file1, file2);
 */
public class BrokerFileGroupAggInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(BrokerFileGroupAggInfo.class);

    private Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();

    // this inner class This class is used to distinguish different combinations of table and partitions
    public static class FileGroupAggKey {
        private long tableId;
        private Set<Long> partitionIds; // empty means partition is not specified

        public FileGroupAggKey(long tableId, List<Long> partitionIds) {
            this.tableId = tableId;
            if (partitionIds != null) {
                this.partitionIds = Sets.newHashSet(partitionIds);
            } else {
                this.partitionIds = Sets.newHashSet();
            }
        }

        public long getTableId() {
            return tableId;
        }

        public Set<Long> getPartitionIds() {
            return partitionIds;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileGroupAggKey)) {
                return false;
            }

            FileGroupAggKey other = (FileGroupAggKey) obj;
            return other.tableId == this.tableId && other.partitionIds.equals(this.partitionIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, partitionIds);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(tableId).append(": ").append(partitionIds).append("]");
            return sb.toString();
        }
    }

    public BrokerFileGroupAggInfo() {

    }

    public void addFileGroup(BrokerFileGroup fileGroup) {
        FileGroupAggKey fileGroupAggKey = new FileGroupAggKey(fileGroup.getTableId(), fileGroup.getPartitionIds());
        List<BrokerFileGroup> fileGroupList = aggKeyToFileGroups.get(fileGroupAggKey);
        if (fileGroupList == null) {
            fileGroupList = Lists.newArrayList();
            aggKeyToFileGroups.put(fileGroupAggKey, fileGroupList);
        }
        fileGroupList.add(fileGroup);
    }

    public Set<Long> getAllTableIds() {
        return aggKeyToFileGroups.keySet().stream().map(k -> k.tableId).collect(Collectors.toSet());
    }

    public Map<FileGroupAggKey, List<BrokerFileGroup>> getAggKeyToFileGroups() {
        return aggKeyToFileGroups;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(aggKeyToFileGroups);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // The pull load source info doesn't need to be persisted.
        // It will be recreated by origin stmt in prepare of load job.
        // write 0 just for compatibility
        out.writeInt(0);
    }

    public void readFields(DataInput in) throws IOException {
        in.readInt();
    }

    public static BrokerFileGroupAggInfo read(DataInput in) throws IOException {
        BrokerFileGroupAggInfo sourceInfo = new BrokerFileGroupAggInfo();
        sourceInfo.readFields(in);
        return sourceInfo;
    }
}
