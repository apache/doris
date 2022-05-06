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

package org.apache.doris.persist;

import org.apache.doris.persist.gson.GsonUtils;
import com.google.gson.annotations.SerializedName;

import java.util.Collection;

/**
 * used for store partitions visit info when a query arrive.
 */

public class VisitPartitionsInfo{
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "partitionNames")
    private Collection<String> partitionNames;

    public VisitPartitionsInfo(String dbName, String tableName, Collection<String> partitionNames) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionNames = partitionNames;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
