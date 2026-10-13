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

package org.apache.doris.job.offset.s3;

import org.apache.doris.job.offset.Offset;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Getter
@Setter
public class S3Offset implements Offset {
    String startFile;
    @SerializedName("endFile")
    String endFile;
    int fileNum;
    private transient List<TBrokerFileStatus> fileStatuses;
    @SerializedName("lastBatch")
    boolean lastBatch;

    @Override
    public String toSerializedJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public boolean isEmpty() {
        return fileStatuses == null || fileStatuses.isEmpty();
    }

    @Override
    public boolean isValidOffset() {
        return StringUtils.isNotBlank(endFile);
    }

    @Override
    public String showRange() {
        return "{\"startFileName\":\"" + startFile + "\",\"endFileName\":\"" + endFile + "\"}";
    }

    @Override
    public String toString() {
        return "{\"startFileName\":\"" + startFile + "\","
                + "\"endFileName\":\"" + endFile + "\",\"fileNum\":" + fileNum + "}";
    }
}
