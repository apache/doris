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

package org.apache.doris.datasource.split;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link PluginDrivenSplit#buildPartitionValues} must NOT collapse an empty partition-value map to {@code null}
 * for a partition-bearing range ({@link ConnectorScanRange#isPartitionBearing()} == true). The generic
 * {@code FileQueryScanNode} treats a {@code null} partition-value list as "parse partition values from the file
 * path" (a Hive-ism): for a connector like Iceberg, whose data files are NOT laid out as {@code key=value}
 * directories, that path parse throws {@code UserException} for a partitioned file that happens to have no
 * identity partition values (e.g. partition-spec evolution from a transform to identity). Legacy
 * {@code IcebergScanNode} never path-parses (it always supplies a non-null empty list). Returning a non-null
 * empty list here reproduces that. Non-partition-bearing ranges (the SPI default — paimon and all others) keep
 * the legacy empty-&gt;null collapse, so this is a zero-regression, opt-in fix.
 */
public class PluginDrivenSplitPartitionValuesTest {

    private static ConnectorScanRange range(Map<String, String> partitionValues, boolean partitionBearing) {
        return new ConnectorScanRange() {
            @Override
            public ConnectorScanRangeType getRangeType() {
                return ConnectorScanRangeType.FILE_SCAN;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getPartitionValues() {
                return partitionValues;
            }

            @Override
            public boolean isPartitionBearing() {
                return partitionBearing;
            }
        };
    }

    @Test
    public void partitionBearingEmptyMapYieldsEmptyListNotNull() {
        // The bug: a partitioned Iceberg file with no identity partition values -> empty map -> (old) null ->
        // FileQueryScanNode path-parses -> UserException. The fix: non-null empty list -> normalizeColumnsFromPath
        // -> no throw. MUTATION: the old `empty -> null` collapse -> getPartitionValues() == null -> red.
        PluginDrivenSplit split = new PluginDrivenSplit(range(Collections.emptyMap(), true));
        Assertions.assertNotNull(split.getPartitionValues(),
                "a partition-bearing range must not collapse an empty map to null (would trigger path parsing)");
        Assertions.assertTrue(split.getPartitionValues().isEmpty());
    }

    @Test
    public void nonPartitionBearingEmptyMapStaysNull() {
        // The SPI default (paimon + every other connector): empty map -> null (legacy collapse preserved).
        // MUTATION: dropping the isPartitionBearing gate -> empty list -> changes paimon behavior -> red.
        PluginDrivenSplit split = new PluginDrivenSplit(range(Collections.emptyMap(), false));
        Assertions.assertNull(split.getPartitionValues(),
                "non-partition-bearing ranges must keep the legacy empty->null collapse (no paimon regression)");
    }

    @Test
    public void nullMapStaysNull() {
        // A genuinely null map is always null regardless of the flag.
        Assertions.assertNull(new PluginDrivenSplit(range(null, true)).getPartitionValues());
        Assertions.assertNull(new PluginDrivenSplit(range(null, false)).getPartitionValues());
    }

    @Test
    public void nonEmptyMapYieldsValuesInOrder() {
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("a", "1");
        parts.put("b", "2");
        PluginDrivenSplit split = new PluginDrivenSplit(range(parts, true));
        Assertions.assertEquals(java.util.Arrays.asList("1", "2"), split.getPartitionValues());
    }
}
