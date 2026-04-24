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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Column handle for MaxCompute tables.
 * Tracks whether the column is a partition column or data column,
 * as the ODPS read session requires separate lists for each.
 */
public class MaxComputeColumnHandle implements ConnectorColumnHandle {
    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final boolean partitionColumn;

    public MaxComputeColumnHandle(String columnName, boolean partitionColumn) {
        this.columnName = columnName;
        this.partitionColumn = partitionColumn;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isPartitionColumn() {
        return partitionColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MaxComputeColumnHandle)) {
            return false;
        }
        MaxComputeColumnHandle that = (MaxComputeColumnHandle) o;
        return partitionColumn == that.partitionColumn
                && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, partitionColumn);
    }

    @Override
    public String toString() {
        return columnName + (partitionColumn ? " [partition]" : "");
    }
}
