// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.DistributionInfo.DistributionInfoType;
import com.baidu.palo.catalog.HashDistributionInfo;
import com.baidu.palo.catalog.DistributionInfo;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.io.Text;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HashDistributionDesc extends DistributionDesc {
    private int numBucket;
    private List<String> distributionColumnNames;

    public HashDistributionDesc() {
        type = DistributionInfoType.HASH;
        distributionColumnNames = Lists.newArrayList();
    }

    public HashDistributionDesc(int numBucket, List<String> distributionColumnNames) {
        type = DistributionInfoType.HASH;
        this.numBucket = numBucket;
        this.distributionColumnNames = distributionColumnNames;
    }

    public List<String> getDistributionColumnNames() {
        return distributionColumnNames;
    }

    public int getBuckets() {
        return numBucket;
    }

    @Override
    public void analyze(Set<String> cols) throws AnalysisException {
        if (numBucket <= 0) {
            throw new AnalysisException("Number of hash distribution is zero.");
        }
        if (distributionColumnNames == null || distributionColumnNames.size() == 0) {
            throw new AnalysisException("Number of hash column is zero.");
        }
        for (String columnName : distributionColumnNames) {
            if (!cols.contains(columnName)) {
                throw new AnalysisException("Distribution column(" + columnName + ") doesn't exist.");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DISTRIBUTED BY HASH(");
        int i = 0;
        for (String columnName : distributionColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")\n");
        stringBuilder.append("BUCKETS ").append(numBucket);
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public DistributionInfo toDistributionInfo(List<Column> columns) throws DdlException {
        List<Column> distributionColumns = Lists.newArrayList();
        
        // check and get distribution column
        for (String colName : distributionColumnNames) {
            boolean find = false;
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    if (!column.isKey()) {
                        throw new DdlException("Distribution column[" + colName + "] is not key column");
                    }

                    distributionColumns.add(column);
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Distribution column[" + colName + "] does not found");
            }
        }

        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(numBucket, distributionColumns);
        return hashDistributionInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeInt(numBucket);
        int count = distributionColumnNames.size();
        out.writeInt(count);
        for (String colName : distributionColumnNames) {
            Text.writeString(out, colName);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numBucket = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            distributionColumnNames.add(Text.readString(in));
        }
    }
}
