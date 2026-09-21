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

package org.apache.doris.connector.spi.write;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Connector-neutral description of the required distribution for a table write. */
public final class ConnectorWriteDistribution {

    public enum Mode {
        EXECUTION_ANY,
        GATHER,
        HASH,
        EXTERNAL_UNPARTITIONED,
        PAIMON_FIXED_BUCKET
    }

    private final Mode mode;
    private final List<String> routeColumns;
    private final int numBuckets;
    private final List<Integer> partitionFieldIndexes;
    private final List<Integer> bucketFieldIndexes;

    private ConnectorWriteDistribution(Mode mode, List<String> routeColumns, int numBuckets,
            List<Integer> partitionFieldIndexes, List<Integer> bucketFieldIndexes) {
        this.mode = mode;
        this.routeColumns = immutable(routeColumns);
        this.numBuckets = numBuckets;
        this.partitionFieldIndexes = immutable(partitionFieldIndexes);
        this.bucketFieldIndexes = immutable(bucketFieldIndexes);
    }

    public static ConnectorWriteDistribution simple(Mode mode) {
        return new ConnectorWriteDistribution(mode, Collections.emptyList(), 0,
                Collections.emptyList(), Collections.emptyList());
    }

    public static ConnectorWriteDistribution hash(List<String> routeColumns) {
        return new ConnectorWriteDistribution(Mode.HASH, routeColumns, 0,
                Collections.emptyList(), Collections.emptyList());
    }

    public static ConnectorWriteDistribution paimonFixedBucket(List<String> routeColumns,
            int numBuckets, List<Integer> partitionFieldIndexes, List<Integer> bucketFieldIndexes) {
        return new ConnectorWriteDistribution(Mode.PAIMON_FIXED_BUCKET, routeColumns, numBuckets,
                partitionFieldIndexes, bucketFieldIndexes);
    }

    private static <T> List<T> immutable(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public Mode getMode() {
        return mode;
    }

    public List<String> getRouteColumns() {
        return routeColumns;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public List<Integer> getPartitionFieldIndexes() {
        return partitionFieldIndexes;
    }

    public List<Integer> getBucketFieldIndexes() {
        return bucketFieldIndexes;
    }
}
