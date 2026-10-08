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

package org.apache.doris.connector.fluss;

import org.apache.fluss.metadata.PartitionInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * How a fluss partition is named on the Doris side — in one place, because two callers must agree on it.
 *
 * <p>Fluss names a partition by joining its values ({@code 20260101$cn}); Doris names it the Hive way
 * ({@code dt=20260101/region=cn}), which is what fe-core parses back out for {@code SHOW PARTITIONS}
 * and what it hands back as the pruned partition set. So the metadata listing and split planning both
 * render this name, and if the two renderings ever drifted apart, planning would match none of the
 * pruned names and silently scan nothing.
 *
 * <p>No escaping is needed: fluss rejects a partition value that is not ASCII alphanumerics, {@code _}
 * or {@code -} (TablePath#detectInvalidName via PartitionUtils#validatePartitionValues), so a value can
 * contain neither {@code =} nor {@code /}, and cannot be null.
 */
final class FlussPartitions {

    private FlussPartitions() {
    }

    /**
     * The scan-side view of {@code partition}: fluss's partition id, the Doris partition name and the
     * per-column values, in partition-key order (fe-core zips values against the partition columns
     * positionally).
     */
    static FlussScanRange.Partition toScanPartition(PartitionInfo partition, List<String> partitionKeys) {
        Map<String, String> spec = partition.getPartitionSpec().getSpecMap();
        Map<String, String> values = new LinkedHashMap<>();
        StringBuilder name = new StringBuilder();
        for (String partitionKey : partitionKeys) {
            String value = spec.get(partitionKey);
            values.put(partitionKey, value);
            if (name.length() > 0) {
                name.append('/');
            }
            name.append(partitionKey).append('=').append(value);
        }
        return FlussScanRange.Partition.of(partition.getPartitionId(), name.toString(), values);
    }
}
