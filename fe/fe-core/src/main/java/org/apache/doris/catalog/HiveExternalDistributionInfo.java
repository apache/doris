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

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

/*
 * Hive Hash Distribution Info
 */
public class HiveExternalDistributionInfo extends HashDistributionInfo {
    @SerializedName(value = "bucketingVersion")
    private final int bucketingVersion;

    public HiveExternalDistributionInfo() {
        bucketingVersion = 2;
    }

    public HiveExternalDistributionInfo(int bucketNum, List<Column> distributionColumns, int bucketingVersion) {
        super(bucketNum, distributionColumns);
        this.bucketingVersion = bucketingVersion;
    }

    public HiveExternalDistributionInfo(int bucketNum, boolean autoBucket,
                                        List<Column> distributionColumns, int bucketingVersion) {
        super(bucketNum, autoBucket, distributionColumns);
        this.bucketingVersion = bucketingVersion;
    }

    public int getBucketingVersion() {
        return bucketingVersion;
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
        HiveExternalDistributionInfo that = (HiveExternalDistributionInfo) o;
        return bucketNum == that.bucketNum
            && sameDistributionColumns(that)
            && bucketingVersion == that.bucketingVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketingVersion);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("type: ").append(type).append("; ");

        builder.append("distribution columns: [");
        for (Column column : getDistributionColumns()) {
            builder.append(column.getName()).append(",");
        }
        builder.append("]; ");

        if (autoBucket) {
            builder.append("bucket num: auto;");
        } else {
            builder.append("bucket num: ").append(bucketNum).append(";");
        }

        builder.append("bucketingVersion: ").append(bucketingVersion).append(";");

        return builder.toString();
    }
}
