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

package org.apache.doris.fs.obj;

import lombok.Getter;

import java.util.List;

/**
 * Result of a paginated object listing operation.
 * Replaces {@code org.apache.doris.cloud.storage.ListObjectsResult}.
 */
public class ListObjectsResult {
    @Getter
    private final List<ObjectFile> objectInfoList;
    @Getter
    private final boolean isTruncated;
    @Getter
    private final String continuationToken;

    public ListObjectsResult(List<ObjectFile> objectInfoList, boolean isTruncated, String continuationToken) {
        this.objectInfoList = objectInfoList;
        this.isTruncated = isTruncated;
        this.continuationToken = continuationToken;
    }

    @Override
    public String toString() {
        return "ListObjectsResult{objectInfoList=" + objectInfoList
                + ", isTruncated=" + isTruncated
                + ", continuationToken='" + continuationToken + "'}";
    }
}
