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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Hash Distribution Info.
 */
public class HashDistributionInfo extends DistributionInfo {
    @SerializedName(value = "distributionColumns")
    private List<Column> distributionColumns;

    public HashDistributionInfo() {
        super();
        this.distributionColumns = new ArrayList<Column>();
    }

    public HashDistributionInfo(int bucketNum, List<Column> distributionColumns) {
        super(DistributionInfoType.HASH, bucketNum);
        this.distributionColumns = distributionColumns;
    }

    public HashDistributionInfo(int bucketNum, boolean autoBucket, List<Column> distributionColumns) {
        super(DistributionInfoType.HASH, bucketNum, autoBucket);
        this.distributionColumns = distributionColumns;
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

    public boolean sameDistributionColumns(HashDistributionInfo other) {
        if (distributionColumns.size() != other.distributionColumns.size()) {
            return false;
        }
        for (int i = 0; i < distributionColumns.size(); ++i) {
            if (!distributionColumns.get(i).equalsForDistribution(other.distributionColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        HashDistributionInfo that = (HashDistributionInfo) o;
        return bucketNum == that.bucketNum && sameDistributionColumns(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), distributionColumns, bucketNum);
    }

    @Override
    public DistributionDesc toDistributionDesc() {
        List<String> distriColNames = Lists.newArrayList();
        for (Column col : distributionColumns) {
            distriColNames.add(col.getName());
        }
        DistributionDesc distributionDesc = new HashDistributionDesc(bucketNum, autoBucket, distriColNames);
        return distributionDesc;
    }

    @Override
    public String getColumnsName() {
        List<String> colNames = Lists.newArrayList();
        for (Column column : distributionColumns) {
            colNames.add("`" + column.getName() + "`");
        }
        return Joiner.on(", ").join(colNames);
    }

    @Override
    public String toSql(boolean forSync) {
        StringBuilder builder = new StringBuilder();
        builder.append("DISTRIBUTED BY HASH(");
        builder.append(getColumnsName());

        if (autoBucket && !forSync) {
            builder.append(") BUCKETS AUTO");
        } else {
            builder.append(") BUCKETS ").append(bucketNum);
        }
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

        if (autoBucket) {
            builder.append("bucket num: auto;");
        } else {
            builder.append("bucket num: ").append(bucketNum).append(";");
        }

        return builder.toString();
    }

    public RandomDistributionInfo toRandomDistributionInfo() {
        return new RandomDistributionInfo(bucketNum);
    }

    public void setDistributionColumns(List<Column> column) {
        this.distributionColumns = column;
    }
}
