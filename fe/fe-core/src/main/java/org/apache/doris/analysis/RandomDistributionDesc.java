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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class RandomDistributionDesc extends DistributionDesc {
    int numBucket;

    public RandomDistributionDesc() {
        type = DistributionInfoType.RANDOM;
    }

    public RandomDistributionDesc(int numBucket) {
        type = DistributionInfoType.RANDOM;
        this.numBucket = numBucket;
    }

    @Override
    public void analyze(Set<String> colSet, List<ColumnDef> columnDefs) throws AnalysisException {
        if (numBucket <= 0) {
            throw new AnalysisException("Number of random distribution should be larger than zero.");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DISTRIBUTED BY RANDOM\n")
                .append("BUCKETS ").append(numBucket);
        return stringBuilder.toString();
    }

    @Override
    public DistributionInfo toDistributionInfo(List<Column> columns) {
        RandomDistributionInfo randomDistributionInfo = new RandomDistributionInfo(numBucket);
        return randomDistributionInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(numBucket);
    }

    public void readFields(DataInput in) throws IOException {
        numBucket = in.readInt();
    }
}
