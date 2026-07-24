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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.resource.Tag;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * This class saves the schema of a colocation group
 */
public class TenantLevelColocateGroupSchema implements Writable {
    @SerializedName(value = "groupId")
    private long groupId;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "distributionColTypes")
    private List<Type> distributionColTypes = Lists.newArrayList();
    @SerializedName(value = "bucketsNum")
    private int bucketsNum;
    @SerializedName(value = "tag")
    private Tag tag;
    @SerializedName(value = "replicaNum")
    private short replicaNum;

    private TenantLevelColocateGroupSchema() {

    }

    public TenantLevelColocateGroupSchema(long groupId, String name, Tag tag, List<Column> distributionCols,
            int bucketsNum, short replicaNum) {
        this.groupId = groupId;
        this.name = name;
        this.tag = tag;
        this.distributionColTypes = distributionCols.stream().map(Column::getType).collect(Collectors.toList());
        this.bucketsNum = bucketsNum;
        this.replicaNum = replicaNum;
    }

    public long getGroupId() {
        return groupId;
    }

    public int getBucketsNum() {
        return bucketsNum;
    }

    public Tag getTag() {
        return tag;
    }

    public short getReplicaNum() {
        return replicaNum;
    }

    public String getName() {
        return name;
    }

    public List<Type> getDistributionColTypes() {
        return distributionColTypes;
    }

    public void checkMasterColocateSchema(OlapTable tbl, Map<String, String> properties) throws DdlException {
        checkMasterDistribution(tbl.getDefaultDistributionInfo());
        checkReplicaAllocation(tbl.getPartitionInfo());
        checkReplicaAllocation(tbl.getDefaultReplicaAllocation());
        checkDynamicPartition(properties, tbl.getDefaultDistributionInfo());
        for (Partition partition : tbl.getAllPartitions()) {
            checkPartitionDistribution(partition.getDistributionInfo(),
                    tbl.getDefaultDistributionInfo().getBucketNum());
        }
    }

    public void checkSlaveColocateSchema(OlapTable tbl, Map<String, String> properties) throws DdlException {
        checkSlaveDistribution(tbl.getDefaultDistributionInfo().getBucketNum(), tbl.getDefaultDistributionInfo());
        checkReplicaAllocation(tbl.getPartitionInfo());
        checkReplicaAllocation(tbl.getDefaultReplicaAllocation());
        checkDynamicPartition(properties, tbl.getDefaultDistributionInfo());
        for (Partition partition : tbl.getAllPartitions()) {
            checkPartitionDistribution(partition.getDistributionInfo(),
                    tbl.getDefaultDistributionInfo().getBucketNum());
        }
    }

    public void checkMasterDistribution(DistributionInfo distributionInfo) throws DdlException {
        HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
        // buckets num
        if (info.getBucketNum() != bucketsNum) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM, info.getBucketNum(),
                    bucketsNum);
        }
        checkDistributionTypes(info);
    }

    public void checkSlaveDistribution(int defaultBucketNum, DistributionInfo distributionInfo) throws DdlException {
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            // buckets num
            if (info.getBucketNum() < bucketsNum || info.getBucketNum() % bucketsNum != 0
                    || info.getBucketNum() != defaultBucketNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_SLAVE_TABLE_MUST_HAS_SAME_BUCKET_NUM, bucketsNum);
            }
            checkDistributionTypes(info);
        }
    }

    public void checkPartitionDistribution(DistributionInfo distributionInfo, int bucketsNum) throws DdlException {
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            // buckets num
            if (info.getBucketNum() != bucketsNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM,
                        info.getBucketNum(), bucketsNum);
            }
            checkDistributionTypes(info);
        }
    }

    private void checkDistributionTypes(HashDistributionInfo info) throws DdlException {
        ColocateGroupSchema.checkDistributionTypes(info, distributionColTypes);
    }

    private void checkReplicaAllocation(PartitionInfo partitionInfo) throws DdlException {
        for (ReplicaAllocation replicaAlloc : partitionInfo.idToReplicaAllocation.values()) {
            checkReplicaAllocation(replicaAlloc);
        }
    }

    public void checkReplicaAllocation(ReplicaAllocation replicaAlloc) throws DdlException {
        if (!replicaAlloc.getAllocMap().containsKey(tag) || replicaAlloc.getReplicaNumByTag(tag) != replicaNum) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_ALLOCATION,
                    replicaAlloc, getReplicaAlloc());
        }
    }

    public static void checkDynamicPartition(Map<String, String> properties,
            DistributionInfo distributionInfo) throws DdlException {
        if (properties.get(DynamicPartitionProperty.BUCKETS) != null) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            int bucketsNum = Integer.parseInt(properties.get(DynamicPartitionProperty.BUCKETS));
            if (info.getBucketNum() != bucketsNum) {
                ErrorReport.reportDdlException(
                        ErrorCode.ERR_DYNAMIC_PARTITION_MUST_HAS_SAME_BUCKET_NUM_WITH_COLOCATE_TABLE,
                        info.getBucketNum(), bucketsNum);
            }
        }
    }

    public static TenantLevelColocateGroupSchema read(DataInput in) throws IOException {
        TenantLevelColocateGroupSchema schema = new TenantLevelColocateGroupSchema();
        schema.readFields(in);
        return schema;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(groupId);
        Text.writeString(out, name);
        out.writeInt(distributionColTypes.size());
        for (Type type : distributionColTypes) {
            ColumnType.write(out, type);
        }
        out.writeInt(bucketsNum);
        this.tag.write(out);
        out.writeShort(replicaNum);
    }

    public void readFields(DataInput in) throws IOException {
        groupId = in.readLong();
        name = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            distributionColTypes.add(ColumnType.read(in));
        }
        bucketsNum = in.readInt();
        tag = Tag.read(in);
        replicaNum = in.readShort();
    }

    public ReplicaAllocation getReplicaAlloc() {
        ReplicaAllocation replicaAllocation = new ReplicaAllocation();
        replicaAllocation.put(tag, replicaNum);
        return replicaAllocation;
    }

    public String getReplicaCreateStmt() {
        return PropertyAnalyzer.TAG_LOCATION + "." + tag.value + ": " + replicaNum;
    }
}
