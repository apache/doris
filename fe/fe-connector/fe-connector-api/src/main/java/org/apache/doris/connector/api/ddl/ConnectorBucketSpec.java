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
 * Bucket / distribution specification carried by
 * {@link ConnectorCreateTableRequest}.
 *
 * <p>{@code algorithm} is a connector-known string. Common values:</p>
 * <ul>
 *   <li>{@code "hive_hash"} — Hive-compatible 32-bit hash.</li>
 *   <li>{@code "iceberg_bucket"} — Iceberg bucket transform.</li>
 *   <li>{@code "doris_default"} — Doris CRC32 distribution.</li>
 * </ul>
 */
public final class ConnectorBucketSpec {

    private final List<String> columns;
    private final int numBuckets;
    private final String algorithm;

    public ConnectorBucketSpec(List<String> columns, int numBuckets,
            String algorithm) {
        this.columns = columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(columns);
        this.numBuckets = numBuckets;
        this.algorithm = Objects.requireNonNull(algorithm, "algorithm");
    }

    public List<String> getColumns() {
        return columns;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorBucketSpec)) {
            return false;
        }
        ConnectorBucketSpec that = (ConnectorBucketSpec) o;
        return numBuckets == that.numBuckets
                && columns.equals(that.columns)
                && algorithm.equals(that.algorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, numBuckets, algorithm);
    }

    @Override
    public String toString() {
        return "ConnectorBucketSpec{algorithm=" + algorithm
                + ", columns=" + columns
                + ", numBuckets=" + numBuckets + "}";
    }
}
