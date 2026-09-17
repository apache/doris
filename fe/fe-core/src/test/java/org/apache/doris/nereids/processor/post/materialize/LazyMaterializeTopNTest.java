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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LazyMaterializeTopNTest {

    @Test
    public void testAliasIsRequiredWhenItsBaseSlotIsMaterialized() {
        SlotReference baseSlot = new SlotReference("base", IntegerType.INSTANCE);
        Slot aliasSlot = new Alias(baseSlot, "alias").toSlot();
        SlotReference independentBaseSlot = new SlotReference("independent", IntegerType.INSTANCE);
        Slot independentAliasSlot = new Alias(independentBaseSlot, "independent_alias").toSlot();
        Relation relation = Mockito.mock(Relation.class);
        Map<Slot, MaterializeSource> materializeMap = ImmutableMap.of(
                aliasSlot, new MaterializeSource(relation, baseSlot),
                independentAliasSlot, new MaterializeSource(relation, independentBaseSlot));

        List<Slot> requiredOutputSlots = LazyMaterializeTopN.collectRequiredOutputSlots(
                materializeMap, ImmutableSet.of(), ImmutableSet.of(baseSlot));

        Assertions.assertEquals(ImmutableList.of(aliasSlot), requiredOutputSlots);
    }

    private static Backend newBackend(long id, String host) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        backend.setBrpcPort(8060);
        return backend;
    }

    @SuppressWarnings("unchecked")
    private static void blackenBackend(long backendId) throws Exception {
        Field field = SimpleScheduler.class.getDeclaredField("blacklistBackends");
        field.setAccessible(true);
        Map<Long, SimpleScheduler.BlackListInfo> blacklist =
                (Map<Long, SimpleScheduler.BlackListInfo>) field.get(null);
        SimpleScheduler.BlackListInfo info = new SimpleScheduler.BlackListInfo();
        blacklist.put(backendId, info);
        for (int i = 0; i < Config.do_add_backend_black_list_threshold_count; i++) {
            info.tryAddBlackList("LazyMaterializeTopNTest");
        }
        Assertions.assertTrue(info.isBlacked());
    }

    @SuppressWarnings("unchecked")
    private static void removeFromBlacklist(long backendId) throws Exception {
        Field field = SimpleScheduler.class.getDeclaredField("blacklistBackends");
        field.setAccessible(true);
        Map<Long, SimpleScheduler.BlackListInfo> blacklist =
                (Map<Long, SimpleScheduler.BlackListInfo>) field.get(null);
        blacklist.remove(backendId);
    }

    // A dead local backend enters the process-global SimpleScheduler blacklist keyed by the
    // numeric backend id. An independently numbered remote backend sharing the same id must
    // still enter the address book, otherwise rows owned by it fail the phase-2 fetch with
    // "failed to find rpc_struct".
    @Test
    void testLocalBlacklistDoesNotEvictRemoteBackendWithSameId() throws Exception {
        Backend localBlacklisted = newBackend(100, "192.168.0.100");
        Backend localHealthy = newBackend(200, "192.168.0.200");
        Backend remoteHealthy = newBackend(100, "10.0.0.100");
        blackenBackend(100);
        try {
            List<Backend> fetchBackends = LazyMaterializeTopN.buildFetchBackends(
                    Arrays.asList(localBlacklisted, localHealthy), Arrays.asList(remoteHealthy));

            List<String> hosts = fetchBackends.stream().map(Backend::getHost).toList();
            Assertions.assertEquals(2, fetchBackends.size());
            // Local blacklisted backend 100 is filtered out by the policy.
            Assertions.assertFalse(hosts.contains("192.168.0.100"));
            // Local healthy backend is kept.
            Assertions.assertTrue(hosts.contains("192.168.0.200"));
            // Remote backend 100 survives the local blacklist and is the only id=100 entry.
            Assertions.assertTrue(hosts.contains("10.0.0.100"));
            Assertions.assertEquals(1, fetchBackends.stream()
                    .filter(backend -> backend.getId() == 100).count());
        } finally {
            removeFromBlacklist(100);
        }
    }

    @Test
    void testDeadRemoteBackendIsSkipped() {
        Backend localHealthy = newBackend(200, "192.168.0.200");
        Backend remoteDead = newBackend(100, "10.0.0.100");
        remoteDead.setAlive(false);
        Backend remoteHealthy = newBackend(300, "10.0.0.300");

        List<Backend> fetchBackends = LazyMaterializeTopN.buildFetchBackends(
                Arrays.asList(localHealthy), Arrays.asList(remoteDead, remoteHealthy));

        List<String> hosts = fetchBackends.stream().map(Backend::getHost).toList();
        Assertions.assertEquals(2, fetchBackends.size());
        Assertions.assertFalse(hosts.contains("10.0.0.100"));
        Assertions.assertTrue(hosts.contains("10.0.0.300"));
        Assertions.assertTrue(hosts.contains("192.168.0.200"));
    }

    // Backend ids of two clusters are independently allocated. On any collision the second
    // phase fetch would silently route rows to a wrong backend, so buildFetchBackends must
    // reject (return null) and the caller skips the lazy materialization rewrite.
    @Test
    void testBackendIdCollisionIsRejected() {
        // local vs remote collision
        Backend localHealthy = newBackend(100, "192.168.0.100");
        Backend remoteHealthy = newBackend(100, "10.0.0.100");
        Assertions.assertNull(LazyMaterializeTopN.buildFetchBackends(
                Arrays.asList(localHealthy), Arrays.asList(remoteHealthy)));

        // remote vs remote collision (two remote catalogs)
        Backend remoteA = newBackend(300, "10.0.0.300");
        Backend remoteB = newBackend(300, "10.1.0.300");
        Assertions.assertNull(LazyMaterializeTopN.buildFetchBackends(
                Arrays.asList(), Arrays.asList(remoteA, remoteB)));
    }

    // LazyMaterializeTopN runs before MergeProjectPostProcessor, whose DefaultPlanRewriter
    // rebuilds ancestors of merged projects via withChildren / withPhysicalPropertiesAndStats.
    // If those copies drop fetchBackends, the translator emits an empty nodes_info and every
    // retained row id fails the phase-2 fetch with "failed to find rpc_struct" — for local
    // tables as well as remote ones.
    @Test
    void testFetchBackendsSurvivePlanCopy() {
        Plan child = Mockito.mock(Plan.class);
        List<Slot> slots = ImmutableList.of(new SlotReference("a", IntegerType.INSTANCE));
        List<Backend> fetchBackends = ImmutableList.of(newBackend(200, "192.168.0.200"));
        PhysicalLazyMaterialize<Plan> node = new PhysicalLazyMaterialize<>(child, slots, slots,
                ImmutableMap.of(), HashBiMap.create(), ImmutableMap.of(), fetchBackends, null, null);

        Plan copiedChild = node.withChildren(ImmutableList.of(child));
        Assertions.assertSame(fetchBackends,
                ((PhysicalLazyMaterialize<?>) copiedChild).getFetchBackends());
        Assertions.assertSame(fetchBackends,
                ((PhysicalLazyMaterialize<?>) node.withPhysicalPropertiesAndStats(null, null))
                        .getFetchBackends());
    }
}
