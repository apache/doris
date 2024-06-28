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
import java.io.IOException;
import java.util.Objects;

/**
 * Random partition.
 */
public class RandomDistributionInfo extends DistributionInfo {
    public RandomDistributionInfo() {
        super();
    }

    public RandomDistributionInfo(int bucketNum) {
        super(DistributionInfoType.RANDOM, bucketNum);
    }

    public RandomDistributionInfo(int bucketNum, boolean autoBucket) {
        super(DistributionInfoType.RANDOM, bucketNum, autoBucket);
    }

    @Override
    public DistributionDesc toDistributionDesc() {
        DistributionDesc distributionDesc = new RandomDistributionDesc(bucketNum, autoBucket);
        return distributionDesc;
    }

    @Override
    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public String toSql(boolean forSync) {
        StringBuilder builder = new StringBuilder();
        if (autoBucket && !forSync) {
            builder.append("DISTRIBUTED BY RANDOM BUCKETS AUTO");
        } else {
            builder.append("DISTRIBUTED BY RANDOM BUCKETS ").append(bucketNum);
        }
        return builder.toString();
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        bucketNum = in.readInt();
    }

    public static DistributionInfo read(DataInput in) throws IOException {
        DistributionInfo distributionInfo = new RandomDistributionInfo();
        distributionInfo.readFields(in);
        return distributionInfo;
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
        RandomDistributionInfo that = (RandomDistributionInfo) o;
        return bucketNum == that.bucketNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketNum);
    }
}
