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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Value-type tests for {@link ConnectorPartitionInfo}, pinning the {@code fileCount} field added for
 * the paimon SHOW PARTITIONS 5-column parity (D-045).
 *
 * <p>{@code fileCount} is the carrier for the legacy FileCount column. Because the class relies on
 * value-based {@code equals}/{@code hashCode}, the field must be threaded through the 7-arg
 * constructor, the getter, AND equals/hashCode — a common place to forget one.</p>
 */
public class ConnectorPartitionInfoTest {

    @Test
    public void sevenArgCtorCarriesFileCount() {
        ConnectorPartitionInfo info = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(),
                /*rowCount*/ 42L, /*sizeBytes*/ 1024L, /*lastModifiedMillis*/ 1700000000000L,
                /*fileCount*/ 7L);
        // WHY: SHOW PARTITIONS' FileCount column reads getFileCount(); it must return the 7th ctor
        // arg, not be confused with rowCount/sizeBytes/lastModifiedMillis. MUTATION: returning any
        // other field, or dropping the assignment (-> 0) -> red.
        Assertions.assertEquals(7L, info.getFileCount());
        Assertions.assertEquals(42L, info.getRowCount());
        Assertions.assertEquals(1024L, info.getSizeBytes());
        Assertions.assertEquals(1700000000000L, info.getLastModifiedMillis());
    }

    @Test
    public void backwardCompatCtorDefaultsFileCountToUnknown() {
        ConnectorPartitionInfo info = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap());
        // WHY: the 3-arg back-compat ctor (used by connectors without per-partition stats, e.g.
        // MaxCompute) must default fileCount to the UNKNOWN sentinel, like the other numeric stats.
        // MUTATION: defaulting to 0 instead of UNKNOWN -> red.
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, info.getFileCount());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, info.getRowCount());
    }

    @Test
    public void equalsAndHashCodeIncludeFileCount() {
        ConnectorPartitionInfo a = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), 1L, 2L, 3L, 7L);
        ConnectorPartitionInfo b = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), 1L, 2L, 3L, 7L);
        ConnectorPartitionInfo differByFileCount = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), 1L, 2L, 3L, 8L);

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        // WHY: value equality must distinguish on fileCount, or two partitions differing only in
        // file count would be (wrongly) treated as equal. MUTATION: omitting fileCount from
        // equals()/hashCode() -> a.equals(differByFileCount) -> red.
        Assertions.assertNotEquals(a, differByFileCount);
    }

    @Test
    public void nullFlagsCtorsCarryPerValueNullFlags() {
        // 4-arg convenience ctor (hive: UNKNOWN stats + connector-supplied per-value NULL flags).
        ConnectorPartitionInfo hive = new ConnectorPartitionInfo(
                "year=__HIVE_DEFAULT_PARTITION__/month=01", Collections.emptyMap(), Collections.emptyMap(),
                Arrays.asList(true, false));
        // WHY: fe-core zips getPartitionValueNullFlags() index-for-index with the parsed values to decide
        // NullLiteral vs typed literal, so the order and values must round-trip. MUTATION: dropping the
        // flags assignment (-> empty) or reordering -> red.
        Assertions.assertEquals(Arrays.asList(true, false), hive.getPartitionValueNullFlags());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, hive.getRowCount());

        // 8-arg ctor (paimon: real stats + NULL flags).
        ConnectorPartitionInfo paimon = new ConnectorPartitionInfo(
                "region=__HIVE_DEFAULT_PARTITION__", Collections.emptyMap(), Collections.emptyMap(),
                1L, 2L, 3L, 4L, Collections.singletonList(true));
        Assertions.assertEquals(Collections.singletonList(true), paimon.getPartitionValueNullFlags());
        Assertions.assertEquals(4L, paimon.getFileCount());
    }

    @Test
    public void backwardCompatCtorsDefaultNullFlagsEmpty() {
        // WHY: connectors that do not opt in (3-arg MaxCompute/iceberg, 7-arg hudi) must default the flags
        // to empty so fe-core treats every value as non-null (unchanged behavior). MUTATION: defaulting to
        // a non-empty list -> red. A null flags arg must normalize to empty (not NPE).
        ConnectorPartitionInfo threeArg = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap());
        ConnectorPartitionInfo sevenArg = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), 1L, 2L, 3L, 4L);
        ConnectorPartitionInfo nullArg = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), null);
        Assertions.assertTrue(threeArg.getPartitionValueNullFlags().isEmpty());
        Assertions.assertTrue(sevenArg.getPartitionValueNullFlags().isEmpty());
        Assertions.assertTrue(nullArg.getPartitionValueNullFlags().isEmpty());
    }

    @Test
    public void equalsAndHashCodeIncludeNullFlags() {
        ConnectorPartitionInfo a = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), Arrays.asList(true, false));
        ConnectorPartitionInfo b = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), Arrays.asList(true, false));
        ConnectorPartitionInfo differByFlags = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), Arrays.asList(false, false));

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        // WHY: two partitions differing only in per-value nullness (a genuine-NULL value vs a literal
        // value that happens to render the same string) must not compare equal. MUTATION: omitting
        // nullFlags from equals()/hashCode() -> a.equals(differByFlags) -> red.
        Assertions.assertNotEquals(a, differByFlags);
    }

    @Test
    public void orderedValuesCtorsCarryValuesAlignedToNullFlags() {
        // 5-arg convenience ctor (hive/iceberg: UNKNOWN stats + connector-supplied ordered values [+ flags]).
        ConnectorPartitionInfo hive = new ConnectorPartitionInfo(
                "nation=cn/city=__HIVE_DEFAULT_PARTITION__", Collections.emptyMap(), Collections.emptyMap(),
                Arrays.asList("cn", "__HIVE_DEFAULT_PARTITION__"), Arrays.asList(false, true));
        // WHY: fe-core zips getOrderedPartitionValues() index-for-index with types + nullFlags INSTEAD of
        // re-parsing the rendered name, so the ordered values must round-trip exactly and stay aligned to the
        // flags. MUTATION: dropping the ordered-values assignment (-> empty, fe-core falls back to the name
        // parse) or reordering -> red.
        Assertions.assertEquals(Arrays.asList("cn", "__HIVE_DEFAULT_PARTITION__"), hive.getOrderedPartitionValues());
        Assertions.assertEquals(Arrays.asList(false, true), hive.getPartitionValueNullFlags());
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, hive.getRowCount());

        // 9-arg full ctor (paimon/hudi: real stats + ordered values + flags).
        ConnectorPartitionInfo paimon = new ConnectorPartitionInfo(
                "region=us", Collections.emptyMap(), Collections.emptyMap(),
                1L, 2L, 3L, 4L, Collections.singletonList("us"), Collections.singletonList(false));
        Assertions.assertEquals(Collections.singletonList("us"), paimon.getOrderedPartitionValues());
        Assertions.assertEquals(4L, paimon.getFileCount());
    }

    @Test
    public void backwardCompatCtorsDefaultOrderedValuesEmpty() {
        // WHY: ctors predating the ordered-values field (3-arg, 7-arg, 8-arg nullFlags, 4-arg) must default
        // ordered values to empty so fe-core falls back to parsing the rendered name (unchanged behavior).
        // MUTATION: defaulting to a non-empty list -> fe-core skips the parse with garbage -> red. A null arg
        // must normalize to empty (not NPE).
        ConnectorPartitionInfo threeArg = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap());
        ConnectorPartitionInfo eightArgFlags = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), 1L, 2L, 3L, 4L,
                Collections.singletonList(true));
        ConnectorPartitionInfo nullArg = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(), null, null);
        Assertions.assertTrue(threeArg.getOrderedPartitionValues().isEmpty());
        Assertions.assertTrue(eightArgFlags.getOrderedPartitionValues().isEmpty());
        Assertions.assertTrue(nullArg.getOrderedPartitionValues().isEmpty());
        Assertions.assertTrue(nullArg.getPartitionValueNullFlags().isEmpty());
    }

    @Test
    public void equalsAndHashCodeIncludeOrderedValues() {
        ConnectorPartitionInfo a = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(),
                Arrays.asList("cn", "bj"), Collections.emptyList());
        ConnectorPartitionInfo b = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(),
                Arrays.asList("cn", "bj"), Collections.emptyList());
        ConnectorPartitionInfo differByValues = new ConnectorPartitionInfo(
                "p1", Collections.emptyMap(), Collections.emptyMap(),
                Arrays.asList("cn", "sh"), Collections.emptyList());

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        // WHY: two partitions with the same rendered name but different ordered values must not compare equal.
        // MUTATION: omitting orderedPartitionValues from equals()/hashCode() -> a.equals(differByValues) -> red.
        Assertions.assertNotEquals(a, differByValues);
    }
}
