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

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * Author: Chenmingyu
 * Date: Jun 10, 2019
 */

/*
 * This class saves the schema of a colocation group
 */
public class ColocateGroupSchema implements Writable {
    private GroupId groupId;
    private List<Column> distributionCols = Lists.newArrayList();
    private int bucketsNum;
    private short replicationNum;

    private ColocateGroupSchema() {

    }

    public ColocateGroupSchema(GroupId groupId, List<Column> distributionCols, int bucketsNum, short replicationNum) {
        this.groupId = groupId;
        this.distributionCols = distributionCols;
        this.bucketsNum = bucketsNum;
        this.replicationNum = replicationNum;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public int getBucketsNum() {
        return bucketsNum;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public List<Column> getDistributionCols() {
        return distributionCols;
    }

    public void checkColocateSchema(OlapTable tbl) throws DdlException {
        checkDistribution(tbl.getDefaultDistributionInfo());
        checkReplicationNum(tbl.getPartitionInfo());
    }

    public void checkDistribution(DistributionInfo distributionInfo) throws DdlException {
        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            // buckets num
            if (info.getBucketNum() != bucketsNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_BUCKNUM, bucketsNum);
            }
            // distribution col size
            if (info.getDistributionColumns().size() != distributionCols.size()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_SAME_DISTRIBUTED_COLUMNS_SIZE,
                        distributionCols.size());
            }
            // distribution col type
            for (int i = 0; i < distributionCols.size(); i++) {
                Type targetColType = distributionCols.get(i).getType();
                if (!targetColType.equals(info.getDistributionColumns().get(i).getType())) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_SAME_DISTRIBUTED_COLUMNS_TYPE,
                            info.getDistributionColumns().get(i).getName(), targetColType);
                }
            }
        }
    }

    public void checkReplicationNum(PartitionInfo partitionInfo) throws DdlException {
        for (Short repNum : partitionInfo.idToReplicationNum.values()) {
            if (repNum != replicationNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_REPLICAT_NUM, replicationNum);
            }
        }
    }

    public void checkReplicationNum(short repNum) throws DdlException {
        if (repNum != replicationNum) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_REPLICAT_NUM, replicationNum);
        }
    }

    public static ColocateGroupSchema read(DataInput in) throws IOException {
        ColocateGroupSchema schema = new ColocateGroupSchema();
        schema.readFields(in);
        return schema;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupId.write(out);
        out.writeInt(distributionCols.size());
        for (Column column : distributionCols) {
            column.write(out);
        }
        out.writeInt(bucketsNum);
        out.writeShort(replicationNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupId = GroupId.read(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Column col = Column.read(in);
            distributionCols.add(col);
        }
        bucketsNum = in.readInt();
        replicationNum = in.readShort();
    }
}
