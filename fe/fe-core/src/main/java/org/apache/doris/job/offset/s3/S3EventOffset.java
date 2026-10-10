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
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class S3EventOffset implements Offset {
    static final int TARGET_BATCH_SERIALIZED_BYTES = 64 * 1024;

    @SerializedName("files")
    private List<String> files;
    private transient List<TBrokerFileStatus> fileStatuses;

    public S3EventOffset() {
    }

    public S3EventOffset(List<String> files) {
        this.files = new ArrayList<>(files);
    }

    public S3EventOffset(List<String> files, List<TBrokerFileStatus> fileStatuses) {
        this(files);
        this.fileStatuses = Collections.unmodifiableList(new ArrayList<>(fileStatuses));
    }

    public List<TBrokerFileStatus> getFileStatuses() {
        return fileStatuses;
    }

    public List<String> getFiles() {
        return files == null ? Collections.emptyList() : Collections.unmodifiableList(files);
    }

    public int serializedSize() {
        return toSerializedJson().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String toSerializedJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public boolean isEmpty() {
        return files == null || files.isEmpty();
    }

    @Override
    public boolean isValidOffset() {
        return !isEmpty() && files.stream().noneMatch(StringUtils::isBlank);
    }

    @Override
    public String showRange() {
        return toSerializedJson();
    }

    @Override
    public String toString() {
        return toSerializedJson();
    }
}
