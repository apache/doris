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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Set;

public class DistributionDesc {
    protected DistributionInfoType type;
    protected int numBucket;
    protected boolean autoBucket;

    public DistributionDesc(int numBucket) {
        this(numBucket, false);
    }

    public DistributionDesc(int numBucket, boolean autoBucket) {
        this.numBucket = numBucket;
        this.autoBucket = autoBucket;
    }

    public int getBuckets() {
        return numBucket;
    }

    public int setBuckets(int numBucket) {
        return this.numBucket = numBucket;
    }

    public boolean isAutoBucket() {
        return autoBucket;
    }

    public void analyze(Set<String> colSet, List<ColumnDef> columnDefs, KeysDesc keysDesc) throws AnalysisException {
        throw new NotImplementedException("analyze not implemented");
    }

    public String toSql() {
        throw new NotImplementedException("toSql not implemented");
    }

    public DistributionInfo toDistributionInfo(List<Column> columns) throws DdlException {
        throw new NotImplementedException("toDistributionInfo not implemented");
    }

    public List<String> getDistributionColumnNames() {
        throw new NotImplementedException("getDistributionColumnNames not implemented");
    }

    public boolean inDistributionColumns(String columnName) {
        return getDistributionColumnNames() != null && getDistributionColumnNames().contains(columnName);
    }
}
