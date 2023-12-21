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

package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

/**
 * MTMVPartitionInfo
 */
public class MTMVPartitionInfo {

    public enum MTMVPartitionType {
        FOLLOW_BASE_TABLE,
        SELF_MANAGE
    }

    @SerializedName("pt")
    MTMVPartitionType partitionType;
    @SerializedName("rt")
    BaseTableInfo relatedTable;
    @SerializedName("rc")
    String relatedCol;
    @SerializedName("pc")
    String partitionCol;

    public MTMVPartitionInfo() {
    }

    public MTMVPartitionInfo(MTMVPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public MTMVPartitionInfo(MTMVPartitionType partitionType,
            String partitionCol) {
        this.partitionType = partitionType;
        this.partitionCol = partitionCol;
    }

    public MTMVPartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(MTMVPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public BaseTableInfo getRelatedTable() {
        return relatedTable;
    }

    public void setRelatedTable(BaseTableInfo relatedTable) {
        this.relatedTable = relatedTable;
    }

    public String getRelatedCol() {
        return relatedCol;
    }

    public void setRelatedCol(String relatedCol) {
        this.relatedCol = relatedCol;
    }

    public String getPartitionCol() {
        return partitionCol;
    }

    public void setPartitionCol(String partitionCol) {
        this.partitionCol = partitionCol;
    }

    @Override
    public String toString() {
        return "MTMVPartitionInfo{"
                + "partitionType=" + partitionType
                + ", relatedTable=" + relatedTable
                + ", relatedCol='" + relatedCol + '\''
                + ", partitionCol='" + partitionCol + '\''
                + '}';
    }

    public String toNameString() {
        if (partitionType == MTMVPartitionType.SELF_MANAGE) {
            return "MTMVPartitionInfo{"
                    + "partitionType=" + partitionType
                    + '}';
        } else {
            return "MTMVPartitionInfo{"
                    + "partitionType=" + partitionType
                    + ", relatedTable=" + relatedTable.getTableName()
                    + ", relatedCol='" + relatedCol + '\''
                    + ", partitionCol='" + partitionCol + '\''
                    + '}';
        }
    }
}
