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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.MetaCacheSizeEstimator;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

/** Immutable partition-view value whose retained size is computed once at construction. */
final class PaimonPartitionView extends AbstractList<ConnectorPartitionInfo> implements RandomAccess {
    private final List<ConnectorPartitionInfo> partitions;
    private final MetaCacheSizeEstimate sizeEstimate;

    PaimonPartitionView(ConnectorTableKey key, List<ConnectorPartitionInfo> partitions) {
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        this.sizeEstimate = MetaCacheSizeEstimator.estimateSafely("paimon_partition_estimator_failure",
                () -> MetaCacheSizeEstimate.complete(
                        PaimonPartitionViewSizeEstimator.estimateEntryOnConstruction(key, this)));
    }

    @Override
    public ConnectorPartitionInfo get(int index) {
        return partitions.get(index);
    }

    @Override
    public int size() {
        return partitions.size();
    }

    MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate;
    }
}
