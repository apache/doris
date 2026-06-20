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
}
