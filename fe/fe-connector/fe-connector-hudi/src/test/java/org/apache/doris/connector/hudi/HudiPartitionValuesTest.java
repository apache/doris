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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Byte-parity tests for {@link HudiScanPlanProvider#parsePartitionValues} against legacy
 * {@code HudiPartitionUtils.parsePartitionValues}. Each case pins WHY the behavior matters for BE-visible
 * partition columns on a snapshot read.
 */
public class HudiPartitionValuesTest {

    @Test
    public void nonHiveStylePositionalPathMapsByPosition() {
        // THE regression this fix closes: Hudi's DEFAULT layout (hive_style_partitioning=false) yields relative
        // paths like "2024/01" with NO "col=" prefix. The old split-on-'=' logic dropped every prefix-less
        // fragment, so the value map was EMPTY -> BE returned NULL partition columns on a plain snapshot read.
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "2024/01", Arrays.asList("year", "month"));

        Assertions.assertEquals(2, values.size(), "both positional fragments must map to their columns");
        Assertions.assertEquals("2024", values.get("year"));
        Assertions.assertEquals("01", values.get("month"));
    }

    @Test
    public void hiveStylePathStripsColumnPrefix() {
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "year=2024/month=01", Arrays.asList("year", "month"));

        Assertions.assertEquals("2024", values.get("year"));
        Assertions.assertEquals("01", values.get("month"));
    }

    @Test
    public void mixedPrefixedAndPositionalFragments() {
        // Legacy decides per fragment (startsWith "col=" or not), so a mixed path must resolve each side.
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "year=2024/01", Arrays.asList("year", "month"));

        Assertions.assertEquals("2024", values.get("year"), "prefixed fragment strips the col= prefix");
        Assertions.assertEquals("01", values.get("month"), "prefix-less fragment maps positionally");
    }

    @Test
    public void unescapesEscapedValues() {
        // Legacy unescaped every value via Hive's FileUtils.unescapePathName; %20 -> space, %2F -> slash. A
        // partition value with an escaped char would otherwise reach BE literally (wrong value).
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "dt=2024-01-01%2012%3A00%3A00", Collections.singletonList("dt"));

        Assertions.assertEquals("2024-01-01 12:00:00", values.get("dt"), "escaped chars must be decoded");
    }

    @Test
    public void singleColumnWholePathFallbackWhenFragmentCountMismatches() {
        // Single partition column, path has more '/' fragments than columns: legacy maps the WHOLE path to the
        // single column (after stripping an optional "col=" prefix), not throw.
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "2024/01/01", Collections.singletonList("dt"));

        Assertions.assertEquals(1, values.size());
        Assertions.assertEquals("2024/01/01", values.get("dt"), "whole path maps to the single column");
    }

    @Test
    public void singleColumnStripsPrefixInWholePathFallback() {
        Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(
                "dt=2024/01/01", Collections.singletonList("dt"));

        Assertions.assertEquals("2024/01/01", values.get("dt"),
                "the leading col= prefix is stripped before the whole-path fallback");
    }

    @Test
    public void multiColumnFragmentCountMismatchFailsLoud() {
        // > 1 partition column and a fragment count that does not match: legacy throws rather than silently
        // producing a partial/wrong value map. Fail loud, matching legacy.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> HudiScanPlanProvider.parsePartitionValues(
                        "2024/01/extra", Arrays.asList("year", "month")));
        Assertions.assertTrue(ex.getMessage().contains("2024/01/extra"),
                "the failure must name the offending partition path");
    }

    @Test
    public void parsePartitionNameUnescapesValuesForPruning() {
        // H1: the PRUNING-decision parse (HudiConnectorMetadata.parsePartitionName, fed the ESCAPED HMS
        // get_partition_names output) must unescape values the same way the scan-side parsePartitionValues does.
        // Otherwise an escaped partition value ("code=US%3ACA") never string-equals the unescaped predicate
        // literal ("US:CA") in matchesPredicates, so the real partition is pruned OUT -> silent row loss. RED
        // before the fix: values stay "US%3ACA" / "a%2Fb".
        Map<String, String> values = HudiConnectorMetadata.parsePartitionName(
                "code=US%3ACA/kind=a%2Fb", Arrays.asList("code", "kind"));

        Assertions.assertEquals("US:CA", values.get("code"), "colon-escaped value must be decoded");
        Assertions.assertEquals("a/b", values.get("kind"), "slash-escaped value must be decoded");
    }

    @Test
    public void hiveDateTimeStringRendersHiveCanonicalText() {
        // H2: a DATETIME/TIMESTAMP predicate literal arrives as a LocalDateTime. It must render as Hive-canonical
        // partition text (space separator, full seconds) so it string-matches the stored partition value in
        // matchesPredicates. String.valueOf(LocalDateTime) yields ISO "2024-01-01T10:00" (T separator, dropped
        // zero seconds) which never matches "2024-01-01 10:00:00" -> the whole table prunes to 0 rows. RED before.
        Assertions.assertEquals("2024-01-01 10:00:00",
                HudiConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 0)));
        // midnight: ISO would collapse to "2024-01-01T00:00"
        Assertions.assertEquals("2024-01-01 00:00:00",
                HudiConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 0, 0, 0)));
        // non-zero seconds: ISO keeps the 'T' separator ("2024-01-01T10:00:30")
        Assertions.assertEquals("2024-01-01 10:00:30",
                HudiConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 30)));
        // sub-second (nano = micros*1000): trailing-zero-trimmed microseconds
        Assertions.assertEquals("2024-01-01 10:00:00.123456",
                HudiConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 123456 * 1000)));
        Assertions.assertEquals("2024-01-01 10:00:00.1",
                HudiConnectorMetadata.hiveDateTimeString(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 100000 * 1000)));
    }

    @Test
    public void emptyPartitionKeysReturnsEmptyForUnpartitionedTable() {
        // Unpartitioned tables reach here with an empty key list and an empty path; the result must be empty
        // (no spurious partition column).
        Assertions.assertTrue(
                HudiScanPlanProvider.parsePartitionValues("", Collections.emptyList()).isEmpty());
        Assertions.assertTrue(
                HudiScanPlanProvider.parsePartitionValues("", null).isEmpty());
    }
}
