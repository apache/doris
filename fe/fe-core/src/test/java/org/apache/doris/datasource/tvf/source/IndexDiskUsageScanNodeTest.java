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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.IndexDiskUsageTableValuedFunction.TabletTarget;
import org.apache.doris.thrift.TIndexDiskUsageMetadataParams;
import org.apache.doris.thrift.TIndexDiskUsageTablet;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexDiskUsageScanNodeTest {

    @Test
    public void testGroupsTabletsByBackend() throws Exception {
        List<TabletTarget> targets = Arrays.asList(target(101, 10, 5), target(102, 10, 5), target(103, 11, 7));
        Map<Long, List<TabletTarget>> groups = IndexDiskUsageScanNode.groupByBackend(targets,
                t -> t.getTabletId() == 102 ? 2L : 1L);
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals(Arrays.asList(101L, 103L), tabletIds(groups.get(1L)));
        Assertions.assertEquals(Arrays.asList(102L), tabletIds(groups.get(2L)));
    }

    @Test
    public void testSelectorFailureIsPropagated() {
        List<TabletTarget> targets = Arrays.asList(target(101, 10, 5));
        UserException e = Assertions.assertThrows(UserException.class,
                () -> IndexDiskUsageScanNode.groupByBackend(targets, t -> {
                    throw new UserException("No queryable replica for tablet 101");
                }));
        Assertions.assertTrue(e.getMessage().contains("No queryable replica for tablet 101"), e.getMessage());
    }

    @Test
    public void testSpreadsTabletsOverQueryableBackends() throws Exception {
        Map<Long, Backend> backends = ImmutableMap.of(
                1L, backend(1L, false), 2L, backend(2L, true), 3L, backend(3L, true));
        List<Replica> replicas = Arrays.asList(replica(3L), replica(1L), replica(2L));
        // Backend 1 is not queryable, so tablets pick from [2, 3] by tablet id.
        Assertions.assertEquals(2L, IndexDiskUsageScanNode.chooseBackend(100L, replicas, backends::get));
        Assertions.assertEquals(3L, IndexDiskUsageScanNode.chooseBackend(101L, replicas, backends::get));
    }

    @Test
    public void testNoQueryableReplicaFails() {
        Map<Long, Backend> backends = ImmutableMap.of(1L, backend(1L, false));
        UserException e = Assertions.assertThrows(UserException.class,
                () -> IndexDiskUsageScanNode.chooseBackend(101L, Arrays.asList(replica(1L)), backends::get));
        Assertions.assertTrue(e.getMessage().contains("No queryable replica for tablet 101"), e.getMessage());
    }

    @Test
    public void testBuildsOneScanRangePerBackend() {
        TIndexDiskUsageMetadataParams params = new TIndexDiskUsageMetadataParams();
        params.setLevel("rowset");
        params.setPositionDetail(true);
        params.setIndexIds(Arrays.asList(1002L));
        params.setPartitionNames(ImmutableMap.of(10L, "p1", 11L, "p2"));
        TMetaScanRange template = new TMetaScanRange();
        template.setMetadataType(TMetadataType.INDEX_DISK_USAGE);
        template.setIndexDiskUsageParams(params);

        Map<Long, List<TabletTarget>> groups = ImmutableMap.of(
                1L, Arrays.asList(target(101, 10, 5), target(103, 11, 7)),
                2L, Arrays.asList(target(102, 10, 5)));
        Map<Long, Backend> backends = ImmutableMap.of(1L, backend(1L, true), 2L, backend(2L, true));

        List<TScanRangeLocations> ranges =
                IndexDiskUsageScanNode.buildScanRangeLocations(template, groups, backends::get);
        Assertions.assertEquals(2, ranges.size());
        for (TScanRangeLocations range : ranges) {
            Assertions.assertEquals(1, range.getLocationsSize());
            TScanRangeLocation location = range.getLocations().get(0);
            TIndexDiskUsageMetadataParams sub = range.getScanRange().getMetaScanRange().getIndexDiskUsageParams();
            Assertions.assertEquals("rowset", sub.getLevel());
            Assertions.assertTrue(sub.isPositionDetail());
            Assertions.assertEquals(Arrays.asList(1002L), sub.getIndexIds());
            Assertions.assertEquals("host" + location.getBackendId(), location.getServer().getHostname());
            Assertions.assertEquals(9060, location.getServer().getPort());
            List<Long> expected = tabletIds(groups.get(location.getBackendId()));
            Assertions.assertEquals(expected,
                    sub.getTablets().stream().map(TIndexDiskUsageTablet::getTabletId).collect(Collectors.toList()));
        }
        Assertions.assertFalse(params.isSetTablets(), "the template must not be mutated");
    }

    private static TabletTarget target(long tabletId, long partitionId, long version) {
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(tabletId);
        return new TabletTarget(tablet, partitionId, version);
    }

    private static List<Long> tabletIds(List<TabletTarget> targets) {
        return targets.stream().map(TabletTarget::getTabletId).collect(Collectors.toList());
    }

    private static Replica replica(long backendId) {
        Replica replica = Mockito.mock(Replica.class);
        Mockito.when(replica.getBackendIdWithoutException()).thenReturn(backendId);
        return replica;
    }

    private static Backend backend(long id, boolean queryAvailable) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(id);
        Mockito.when(backend.getHost()).thenReturn("host" + id);
        Mockito.when(backend.getBePort()).thenReturn(9060);
        Mockito.when(backend.isQueryAvailable()).thenReturn(queryAvailable);
        return backend;
    }
}
