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
import org.apache.doris.common.io.Writable;

import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class DistributionInfo implements Writable {

    public enum DistributionInfoType {
        HASH,
        RANDOM
    }

    // for Gson runtime type adaptor
    @SerializedName(value = "typeStr")
    protected String typeStr;
    @SerializedName(value = "type")
    protected DistributionInfoType type;
    
    public DistributionInfo() {
        // for persist
    }

    public DistributionInfo(DistributionInfoType type) {
        this.type = type;
        this.typeStr = this.type.name();
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

    public DistributionDesc toDistributionDesc() {
        throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    public void readFields(DataInput in) throws IOException {
        type = DistributionInfoType.valueOf(Text.readString(in));
    }

    public String toSql() {
        return "";
    }

    public boolean equals(DistributionInfo info) {
        return false;
    }
}
