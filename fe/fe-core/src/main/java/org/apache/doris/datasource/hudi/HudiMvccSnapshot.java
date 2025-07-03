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

package org.apache.doris.datasource.hudi;

import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

/**
 * Implementation of MvccSnapshot for Hudi tables that maintains partition values
 * for MVCC (Multiversion Concurrency Control) operations.
 * This class is immutable to ensure thread safety.
 */
public class HudiMvccSnapshot implements MvccSnapshot {
    private final TablePartitionValues tablePartitionValues;
    private final long timestamp;

    /**
     * Creates a new HudiMvccSnapshot with the specified partition values.
     *
     * @param tablePartitionValues The partition values for the snapshot
     * @throws IllegalArgumentException if tablePartitionValues is null
     */
    public HudiMvccSnapshot(TablePartitionValues tablePartitionValues, Long timeStamp) {
        if (tablePartitionValues == null) {
            throw new IllegalArgumentException("TablePartitionValues cannot be null");
        }
        this.timestamp = timeStamp;
        this.tablePartitionValues = tablePartitionValues;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets the table partition values associated with this snapshot.
     *
     * @return The immutable TablePartitionValues object
     */
    public TablePartitionValues getTablePartitionValues() {
        return tablePartitionValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HudiMvccSnapshot that = (HudiMvccSnapshot) o;
        return tablePartitionValues.equals(that.tablePartitionValues);
    }

    @Override
    public int hashCode() {
        return tablePartitionValues.hashCode();
    }

    @Override
    public String toString() {
        return String.format("HudiMvccSnapshot{tablePartitionValues=%s}", tablePartitionValues);
    }
}
