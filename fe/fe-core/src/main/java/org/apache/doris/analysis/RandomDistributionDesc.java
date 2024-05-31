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
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.common.AnalysisException;

import java.util.List;
import java.util.Set;

public class RandomDistributionDesc extends DistributionDesc {
    public RandomDistributionDesc(int numBucket) {
        super(numBucket);
        type = DistributionInfoType.RANDOM;
    }

    public RandomDistributionDesc(int numBucket, boolean autoBucket) {
        super(numBucket, autoBucket);
        type = DistributionInfoType.RANDOM;
    }

    @Override
    public void analyze(Set<String> colSet, List<ColumnDef> columnDefs, KeysDesc keysDesc) throws AnalysisException {
        if (numBucket <= 0) {
            throw new AnalysisException("Number of random distribution should be greater than zero.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DISTRIBUTED BY RANDOM\n")
                .append("BUCKETS ");
        if (autoBucket) {
            stringBuilder.append("AUTO");
        } else {
            stringBuilder.append(numBucket);
        }
        return stringBuilder.toString();
    }

    @Override
    public DistributionInfo toDistributionInfo(List<Column> columns) {
        RandomDistributionInfo randomDistributionInfo = new RandomDistributionInfo(numBucket, autoBucket);
        return randomDistributionInfo;
    }

    @Override
    public List<String> getDistributionColumnNames() {
        return null;
    }
}
