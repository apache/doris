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
import org.apache.doris.analysis.RandomDistributionDesc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Random partition.
 */
public class RandomDistributionInfo extends DistributionInfo {
    
    private int bucketNum;

    public RandomDistributionInfo() {
        super();
    }
    
    public RandomDistributionInfo(int bucketNum) {
        super(DistributionInfoType.RANDOM);
        this.bucketNum = bucketNum;
    }
    
    @Override
    public DistributionDesc toDistributionDesc() {
        DistributionDesc distributionDesc = new RandomDistributionDesc(bucketNum);
        return distributionDesc;
    }

    @Override
    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("DISTRIBUTED BY RANDOM BUCKETS ").append(bucketNum);
        return builder.toString();
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(bucketNum);
    }
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        bucketNum = in.readInt();
    }

    public static DistributionInfo read(DataInput in) throws IOException {
        DistributionInfo distributionInfo = new RandomDistributionInfo();
        distributionInfo.readFields(in);
        return distributionInfo;
    }
    
    public boolean equals(DistributionInfo info) {
        if (this == info) {
            return true;
        }

        if (!(info instanceof RandomDistributionInfo)) {
            return false;
        }

        RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) info;

        return type == randomDistributionInfo.type
                && bucketNum == randomDistributionInfo.bucketNum;
    }
}
