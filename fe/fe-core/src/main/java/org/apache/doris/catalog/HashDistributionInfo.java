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

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hash Distribution Info.
 */
public class HashDistributionInfo extends DistributionInfo {
    @SerializedName(value = "distributionColumns")
    private List<Column> distributionColumns;
    @SerializedName(value = "bucketNum")
    private int bucketNum;

    public HashDistributionInfo() {
        super();
        this.distributionColumns = new ArrayList<Column>();
    }

    public HashDistributionInfo(int bucketNum, List<Column> distributionColumns) {
        super(DistributionInfoType.HASH);
        this.distributionColumns = distributionColumns;
        this.bucketNum = bucketNum;
    }

    public List<Column> getDistributionColumns() {
        return distributionColumns;
    }

    @Override
    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        int columnCount = distributionColumns.size();
        out.writeInt(columnCount);
        for (Column column : distributionColumns) {
            column.write(out);
        }
        out.writeInt(bucketNum);
    }
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int columnCount = in.readInt();
        for (int i = 0; i < columnCount; i++) {
            Column column = Column.read(in);
            distributionColumns.add(column);
        }
        bucketNum = in.readInt();
    }

    public static DistributionInfo read(DataInput in) throws IOException {
        DistributionInfo distributionInfo = new HashDistributionInfo();
        distributionInfo.readFields(in);
        return distributionInfo;
    }

    public boolean equals(DistributionInfo info) {
        if (this == info) {
            return true;
        }

        if (!(info instanceof HashDistributionInfo)) {
            return false;
        }

        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) info;

        return type == hashDistributionInfo.type
                && bucketNum == hashDistributionInfo.bucketNum
                && distributionColumns.equals(hashDistributionInfo.distributionColumns);
    }

    @Override
    public DistributionDesc toDistributionDesc() {
        List<String> distriColNames = Lists.newArrayList();
        for (Column col : distributionColumns) {
            distriColNames.add(col.getName());
        }
        DistributionDesc distributionDesc = new HashDistributionDesc(bucketNum, distriColNames);
        return distributionDesc;
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("DISTRIBUTED BY HASH(");

        List<String> colNames = Lists.newArrayList();
        for (Column column : distributionColumns) {
            colNames.add("`" + column.getName() + "`");
        }
        String colList = Joiner.on(", ").join(colNames);
        builder.append(colList);

        builder.append(") BUCKETS ").append(bucketNum);
        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("type: ").append(type).append("; ");

        builder.append("distribution columns: [");
        for (Column column : distributionColumns) {
            builder.append(column.getName()).append(",");
        }
        builder.append("]; ");

        builder.append("bucket num: ").append(bucketNum).append("; ");;

        return builder.toString();
    }

    public RandomDistributionInfo toRandomDistributionInfo() {
        return new RandomDistributionInfo(bucketNum);
    }
}
