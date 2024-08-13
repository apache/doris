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
import org.apache.doris.common.io.Text;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.NotImplementedException;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

public abstract class DistributionInfo {

    public enum DistributionInfoType {
        HASH,
        RANDOM
    }

    // for Gson runtime type adaptor
    @SerializedName(value = "type")
    protected DistributionInfoType type;

    @SerializedName(value = "bucketNum")
    protected int bucketNum;

    @SerializedName(value = "autoBucket")
    protected boolean autoBucket;

    public DistributionInfo() {
        // for persist
    }

    public DistributionInfo(DistributionInfoType type) {
        this(type, 0, false);
    }

    public DistributionInfo(DistributionInfoType type, int bucketNum) {
        this(type, bucketNum, false);
    }

    public DistributionInfo(DistributionInfoType type, int bucketNum, boolean autoBucket) {
        this.type = type;
        this.bucketNum = bucketNum;
        this.autoBucket = autoBucket;
    }

    public DistributionInfoType getType() {
        return type;
    }

    public int getBucketNum() {
        // should override in sub class
        throw new NotImplementedException("not implemented");
    }

    public void setBucketNum(int bucketNum) {
        // should override in sub class
        throw new NotImplementedException("not implemented");
    }

    public void markAutoBucket() {
        autoBucket = true;
    }

    public DistributionDesc toDistributionDesc() {
        throw new NotImplementedException("toDistributionDesc not implemented");
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        type = DistributionInfoType.valueOf(Text.readString(in));
    }

    public String toSql(boolean forSync) {
        return "";
    }

    public String toSql() {
        return toSql(false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionInfo that = (DistributionInfo) o;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}
