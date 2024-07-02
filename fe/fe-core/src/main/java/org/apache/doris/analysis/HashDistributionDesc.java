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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class HashDistributionDesc extends DistributionDesc {
    private List<String> distributionColumnNames;

    public HashDistributionDesc(int numBucket, List<String> distributionColumnNames) {
        super(numBucket);
        type = DistributionInfoType.HASH;
        this.distributionColumnNames = distributionColumnNames;
    }

    public HashDistributionDesc(int numBucket, boolean autoBucket, List<String> distributionColumnNames) {
        super(numBucket, autoBucket);
        type = DistributionInfoType.HASH;
        this.distributionColumnNames = distributionColumnNames;
    }

    @Override
    public List<String> getDistributionColumnNames() {
        return distributionColumnNames;
    }

    @Override
    public void analyze(Set<String> colSet, List<ColumnDef> columnDefs, KeysDesc keysDesc) throws AnalysisException {
        if (numBucket <= 0) {
            throw new AnalysisException("Number of hash distribution should be greater than zero.");
        }
        if (distributionColumnNames == null || distributionColumnNames.size() == 0) {
            throw new AnalysisException("Number of hash column should be larger than zero.");
        }
        Set<String> distColSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String columnName : distributionColumnNames) {
            if (!colSet.contains(columnName)) {
                throw new AnalysisException("Distribution column(" + columnName + ") doesn't exist.");
            }
            if (!distColSet.add(columnName)) {
                throw new AnalysisException("Duplicated distribution column " + columnName);
            }
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equalsIgnoreCase(columnName)) {
                    if (!columnDef.isKey() && (keysDesc.getKeysType() == KeysType.UNIQUE_KEYS
                            || keysDesc.getKeysType() == KeysType.AGG_KEYS)) {
                        throw new AnalysisException("Distribution column[" + columnName + "] is not key column");
                    }
                    break;
                }
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
        if (autoBucket) {
            stringBuilder.append("BUCKETS AUTO");
        } else {
            stringBuilder.append("BUCKETS ").append(numBucket);
        }
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
                    if (column.getType().isScalarType(PrimitiveType.STRING)) {
                        throw new DdlException("String Type should not be used in distribution column["
                                + column.getName() + "].");
                    } else if (column.getType().isArrayType()) {
                        throw new DdlException("Array Type should not be used in distribution column["
                                + column.getName() + "].");
                    } else if (column.getType().isMapType()) {
                        throw new DdlException("Map Type should not be used in distribution column["
                                + column.getName() + "].");
                    } else if (column.getType().isStructType()) {
                        throw new DdlException("Struct Type should not be used in distribution column["
                                + column.getName() + "].");
                    } else if (column.getType().isFloatingPointType()) {
                        throw new DdlException("Floating point type should not be used in distribution column["
                                + column.getName() + "].");
                    }

                    // distribution info and base columns persist seperately inside OlapTable, so we need deep copy
                    // to avoid modify table columns also modify columns inside distribution info.
                    distributionColumns.add(new Column(column));
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Distribution column[" + colName + "] does not found");
            }
        }

        HashDistributionInfo hashDistributionInfo =
                                new HashDistributionInfo(numBucket, autoBucket, distributionColumns);
        return hashDistributionInfo;
    }
}
