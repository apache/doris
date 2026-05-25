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

package org.apache.doris.connector.api.ddl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Initial value definition for a Doris-style {@code LIST} or {@code RANGE}
 * partition declared in a {@code CREATE TABLE} statement.
 *
 * <p>For {@code LIST} partitions, {@code values} contains the literal list of
 * permitted values (each inner list is one tuple matching the partition columns).
 * For {@code RANGE} partitions, {@code values} contains exactly two tuples
 * representing the [lower, upper) bound.</p>
 */
public final class ConnectorPartitionValueDef {

    private final String partitionName;
    private final List<List<String>> values;

    public ConnectorPartitionValueDef(String partitionName,
            List<List<String>> values) {
        this.partitionName = Objects.requireNonNull(partitionName, "partitionName");
        this.values = values == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(values);
    }

    public String getPartitionName() {
        return partitionName;
    }

    public List<List<String>> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPartitionValueDef)) {
            return false;
        }
        ConnectorPartitionValueDef that = (ConnectorPartitionValueDef) o;
        return partitionName.equals(that.partitionName)
                && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, values);
    }

    @Override
    public String toString() {
        return "ConnectorPartitionValueDef{name='" + partitionName
                + "', values=" + values + "}";
    }
}
