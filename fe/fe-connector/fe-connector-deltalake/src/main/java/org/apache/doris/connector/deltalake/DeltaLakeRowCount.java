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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.data.Row;

import java.util.OptionalLong;

/** Logical rows in a complete, unfiltered snapshot; shared by CBO statistics and exact COUNT pushdown. */
final class DeltaLakeRowCount {
    private long count;
    private boolean complete = true;

    void add(Row file) {
        OptionalLong records = DeltaKernelCompat.numRecords(file);
        long deleted = DeltaKernelCompat.deletionCardinality(file);
        if (records.isEmpty()) {
            complete = false;
        } else {
            if (deleted < 0 || records.getAsLong() < deleted) {
                throw new DorisConnectorException("Invalid Delta row count or deletion cardinality for "
                        + DeltaKernelCompat.fileStatus(file).getPath());
            }
            count = Math.addExact(count, records.getAsLong() - deleted);
        }
    }

    OptionalLong value() {
        return complete ? OptionalLong.of(count) : OptionalLong.empty();
    }
}
