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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CloudColocatePlacementTest {
    private final boolean oldEnableCloudColocateConsistentHash = Config.enable_cloud_colocate_consistent_hash;

    @AfterEach
    public void tearDown() {
        Config.enable_cloud_colocate_consistent_hash = oldEnableCloudColocateConsistentHash;
    }

    @Test
    public void testAddAndRemoveOneBackendMovesFarFewerBucketsThanModulo() {
        long groupId = 100L;
        int bucketNum = 4096;
        long[] originalBeIds = range(1, 16);
        long[] addedBeIds = range(1, 17);
        long[] removedBeIds = range(1, 15);

        int hrwAddMoved = changedBucketsByHrw(groupId, bucketNum, originalBeIds, addedBeIds);
        int hrwRemoveMoved = changedBucketsByHrw(groupId, bucketNum, originalBeIds, removedBeIds);
        int moduloAddMoved = changedBucketsByModulo(groupId, bucketNum, originalBeIds, addedBeIds);
        int moduloRemoveMoved = changedBucketsByModulo(groupId, bucketNum, originalBeIds, removedBeIds);

        Assertions.assertTrue(hrwAddMoved < bucketNum / 8,
                "HRW should move about 1/N buckets after adding one BE, but moved " + hrwAddMoved);
        Assertions.assertTrue(hrwRemoveMoved < bucketNum / 8,
                "HRW should move about 1/N buckets after removing one BE, but moved " + hrwRemoveMoved);
        Assertions.assertTrue(moduloAddMoved > bucketNum * 9 / 10,
                "Modulo should move almost all buckets after adding one BE, but moved " + moduloAddMoved);
        Assertions.assertTrue(moduloRemoveMoved > bucketNum * 9 / 10,
                "Modulo should move almost all buckets after removing one BE, but moved " + moduloRemoveMoved);
    }

    @Test
    public void testTieBreakPicksSmallerBackendId() {
        long[] beIds = new long[] {30L, 20L, 10L};

        long pickedBeId = CloudColocatePlacement.pickBackendId(100L, 1L, beIds, (groupId, idx, beId) -> 1L);

        Assertions.assertEquals(10L, pickedBeId);
    }

    @Test
    public void testCacheInvalidatesWhenBackendSetChanges() {
        CloudSystemInfoService infoService = new CloudSystemInfoService();
        GroupId groupId = new GroupId(1L, 100L);
        List<Long> originalBeIds = Arrays.asList(1L, 2L, 3L);
        List<Long> addedBeIds = Arrays.asList(1L, 2L, 3L, 4L);
        int bucketNum = 128;

        int changed = 0;
        for (int idx = 0; idx < bucketNum; idx++) {
            long original = infoService.getCloudColocateHrwBeId(groupId, "cluster0", originalBeIds, bucketNum, idx);
            long cached = infoService.getCloudColocateHrwBeId(groupId, "cluster0", originalBeIds, bucketNum, idx);
            long afterAdd = infoService.getCloudColocateHrwBeId(groupId, "cluster0", addedBeIds, bucketNum, idx);
            Assertions.assertEquals(original, cached);
            if (original != afterAdd) {
                changed++;
            }
        }

        Assertions.assertTrue(changed > 0, "Changing BE set should invalidate cached placement");
        Assertions.assertTrue(changed < bucketNum / 2, "HRW should not rebuild as modulo-style reshuffle");
    }

    @Test
    public void testCachePlacementSameForSameBackendSetInDifferentOrder() {
        CloudSystemInfoService infoService = new CloudSystemInfoService();
        GroupId groupId = new GroupId(1L, 100L);
        List<Long> originalBeIds = Arrays.asList(1L, 2L, 3L);
        List<Long> reorderedBeIds = Arrays.asList(3L, 1L, 2L);
        int bucketNum = 128;

        for (int idx = 0; idx < bucketNum; idx++) {
            long original = infoService.getCloudColocateHrwBeId(groupId, "cluster0", originalBeIds, bucketNum, idx);
            long reordered = infoService.getCloudColocateHrwBeId(groupId, "cluster0", reorderedBeIds, bucketNum, idx);
            Assertions.assertEquals(original, reordered);
        }
    }

    @Test
    public void testConfigOffUsesLegacyModuloResult() {
        Config.enable_cloud_colocate_consistent_hash = false;
        GroupId groupId = new GroupId(1L, 100L);
        List<Backend> backends = createBackends(10L, 20L, 30L);
        CloudReplica replica = new CloudReplica(1L, -1L, ReplicaState.NORMAL, 0L, 0, 1L, 2L, 3L, 4L, 5L);

        long pickedBeId = replica.pickColocatedBackend(new CloudSystemInfoService(), groupId, "cluster0", backends)
                .getId();
        long expectedBeId = pickModulo(groupId.grpId, 5L, backends.stream().mapToLong(Backend::getId).toArray());

        Assertions.assertEquals(expectedBeId, pickedBeId);
    }

    private static long[] range(int fromInclusive, int toExclusive) {
        long[] values = new long[toExclusive - fromInclusive];
        for (int i = 0; i < values.length; i++) {
            values[i] = fromInclusive + i;
        }
        return values;
    }

    private static int changedBucketsByHrw(long groupId, int bucketNum, long[] originalBeIds, long[] changedBeIds) {
        int changed = 0;
        for (int idx = 0; idx < bucketNum; idx++) {
            long before = CloudColocatePlacement.pickBackendId(groupId, idx, originalBeIds);
            long after = CloudColocatePlacement.pickBackendId(groupId, idx, changedBeIds);
            if (before != after) {
                changed++;
            }
        }
        return changed;
    }

    private static int changedBucketsByModulo(long groupId, int bucketNum, long[] originalBeIds, long[] changedBeIds) {
        int changed = 0;
        for (int idx = 0; idx < bucketNum; idx++) {
            long before = pickModulo(groupId, idx, originalBeIds);
            long after = pickModulo(groupId, idx, changedBeIds);
            if (before != after) {
                changed++;
            }
        }
        return changed;
    }

    private static long pickModulo(long groupId, long idx, long[] beIds) {
        HashCode hashCode = Hashing.murmur3_128().hashLong(groupId);
        long index = (hashCode.asLong() + idx) % beIds.length;
        index = (index + beIds.length) % beIds.length;
        return beIds[(int) index];
    }

    private static List<Backend> createBackends(long... beIds) {
        List<Backend> backends = new ArrayList<>();
        for (long beId : beIds) {
            backends.add(new Backend(beId, "127.0.0." + beId, 9050));
        }
        return backends;
    }
}
