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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Collections;
import java.util.List;

/**
 * Doris addition: the outcome of one {@code get_partitions_by_filter} page, carrying BOTH the hook-filtered
 * partitions and the RAW (pre-{@code MetaStoreFilterHook}) page size.
 *
 * <p>WHY the raw size is load-bearing: the concrete client caps the raw metastore response first and applies
 * {@code filterHook.filterPartitions} afterwards, so the post-hook list size cannot distinguish "the raw page
 * was truncated at the cap" from "the raw page was complete and the hook hid an entry". A caller that must not
 * silently drop matching partitions - Doris's direct-HMS partition pruning - therefore has to decide on the
 * raw size, which this holder preserves.</p>
 */
public final class HmsRawPartitionFilterPage {

    private final List<Partition> partitions;
    private final int rawCount;

    public HmsRawPartitionFilterPage(List<Partition> partitions, int rawCount) {
        this.partitions = Collections.unmodifiableList(partitions);
        this.rawCount = rawCount;
    }

    /** The hook-filtered partitions, i.e. what a plain {@code listPartitionsByFilter} would return. */
    public List<Partition> getPartitions() {
        return partitions;
    }

    /** The number of partitions the raw metastore call returned, BEFORE the filter hook was applied. */
    public int getRawCount() {
        return rawCount;
    }
}
