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

package org.apache.doris.statistics;

import com.google.gson.annotations.SerializedName;

import java.util.Set;

public class UpdatePartitionStatsTarget {

    @SerializedName("catalogId")
    public final long catalogId;

    @SerializedName("dbId")
    public final long dbId;

    @SerializedName("tableId")
    public final long tableId;

    @SerializedName("indexId")
    public final long indexId;

    @SerializedName("column")
    public final String columnName;

    @SerializedName("partitions")
    public final Set<String> partitions;

    public UpdatePartitionStatsTarget(long catalogId, long dbId, long tableId,
                                      long indexId, String columnName, Set<String> partitions) {
        this.catalogId = catalogId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.columnName = columnName;
        this.partitions = partitions;
    }
}
