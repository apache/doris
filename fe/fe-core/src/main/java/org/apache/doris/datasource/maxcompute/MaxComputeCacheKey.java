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

package org.apache.doris.datasource.maxcompute;

import lombok.Data;

import java.util.Objects;

@Data
public class MaxComputeCacheKey {
    private final String dbName;
    private final String tblName;
    private String partitionSpec; // optional

    public MaxComputeCacheKey(String dbName, String tblName) {
        this(dbName, tblName, null);
    }

    public MaxComputeCacheKey(String dbName, String tblName, String partitionSpec) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.partitionSpec = partitionSpec;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MaxComputeCacheKey)) {
            return false;
        }
        boolean partitionEquals = true;
        if (partitionSpec != null) {
            partitionEquals = partitionSpec.equals(((MaxComputeCacheKey) obj).partitionSpec);
        }
        return partitionEquals && dbName.equals(((MaxComputeCacheKey) obj).dbName)
                && tblName.equals(((MaxComputeCacheKey) obj).tblName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tblName);
    }

    @Override
    public String toString() {
        return "TablePartitionKey{" + "dbName='" + dbName + '\'' + ", tblName='" + tblName + '\'' + '}';
    }
}
