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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Which partitions a snapshot read answers with the table as of the stream offset, rather than with the table
 * as it is now.
 *
 * <p>The read is built differently per key type -- a merge-on-write table splits its partitions into those
 * read directly and those rebuilt from the binlog into the image at the offset, a duplicate-key one reads all
 * of them bounded by the offset -- so the two are pinned separately. What the refresh that asks this may not
 * do is record such a partition as holding the table's current state.
 */
public class OlapTableStreamSnapshotReadTest {

    @Test
    public void testAMergeOnWritePartitionBehindItsOffsetIsReadAsAnOlderImage() {
        // The offset of the first partition has not reached its end, so a read of it answers with the image at
        // that offset; the second is read directly.
        Fixture fixture = fixture(KeysType.UNIQUE_KEYS, Map.of(1L, 100L, 2L, 200L),
                partition(1L, 200L, true), partition(2L, 200L, true));

        Assertions.assertTrue(fixture.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(1L, 2L)));
        Assertions.assertFalse(fixture.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(2L)));
    }

    @Test
    public void testADuplicateKeyPartitionBehindItsOffsetIsReadAsAnOlderImage() {
        Fixture fixture = fixture(KeysType.DUP_KEYS, Map.of(1L, 100L, 2L, 200L),
                partition(1L, 200L, true), partition(2L, 200L, true));

        Assertions.assertTrue(fixture.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(1L, 2L)));
        Assertions.assertFalse(fixture.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(2L)));

        // A partition with no consumption baseline is not behind an offset it does not have: the read is
        // bounded by nothing and answers with the table as it is. Counting it as an older image records
        // partitions that are current, which costs a rebuild each.
        Fixture baselineLess = fixture(KeysType.DUP_KEYS, Map.of(2L, 200L),
                partition(1L, 200L, true), partition(2L, 200L, true));
        Assertions.assertFalse(baselineLess.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(1L, 2L)));

        // One that has been consumed up to its end is not behind it either.
        Fixture consumed = fixture(KeysType.DUP_KEYS, Map.of(1L, 200L, 2L, 200L),
                partition(1L, 200L, true), partition(2L, 200L, true));
        Assertions.assertFalse(consumed.wrapper.readsSnapshotOfAnOlderImage(Lists.newArrayList(1L, 2L)));
    }

    private static Partition partition(long id, long tso, boolean hasData) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(id);
        Mockito.when(partition.getTso()).thenReturn(tso);
        Mockito.when(partition.hasData()).thenReturn(hasData);
        return partition;
    }

    private static class Fixture {
        private final OlapTableStream stream;
        private final OlapTableStreamWrapper wrapper;

        private Fixture(OlapTableStream stream, OlapTableStreamWrapper wrapper) {
            this.stream = stream;
            this.wrapper = wrapper;
        }
    }

    private static Fixture fixture(KeysType keysType, Map<Long, Long> offsets, Partition... partitions) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getKeysType()).thenReturn(keysType);
        Mockito.when(table.getQualifiedDbName()).thenReturn("db1");
        Map<Long, Partition> byId = new HashMap<>();
        for (Partition partition : partitions) {
            byId.put(partition.getId(), partition);
        }
        Mockito.when(table.getPartition(Mockito.anyLong()))
                .thenAnswer(invocation -> byId.get(invocation.getArgument(0)));
        OlapTableStream stream = Mockito.spy(new OlapTableStream());
        Mockito.doReturn(table).when(stream).getBaseTableNullable();
        // The offsets have to be in place before the wrapper is built: it reads them into the map the read of
        // a snapshot is built from.
        Deencapsulation.setField(stream, "partitionOffset", new HashMap<>(offsets));
        Deencapsulation.setField(stream, "historicalPartitionTSO", new HashMap<Long, Long>());
        return new Fixture(stream, new OlapTableStreamWrapper(stream, table,
                Lists.newArrayList(byId.keySet())));
    }
}
