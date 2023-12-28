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

package org.apache.doris.clone;

import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.ColocateTableCheckerAndBalancer.BackendBuckets;
import org.apache.doris.clone.ColocateTableCheckerAndBalancer.BucketStatistic;
import org.apache.doris.clone.ColocateTableCheckerAndBalancer.GlobalColocateStatistic;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColocateTableCheckerAndBalancerTest {
    private ColocateTableCheckerAndBalancer balancer = ColocateTableCheckerAndBalancer.getInstance();

    private Backend backend1;
    private Backend backend2;
    private Backend backend3;
    private Backend backend4;
    private Backend backend5;
    private Backend backend6;
    private Backend backend7;
    private Backend backend8;
    private Backend backend9;

    private Map<Long, Double> mixLoadScores;

    @Before
    public void setUp() {
        backend1 = new Backend(1L, "192.168.1.1", 9050);
        backend2 = new Backend(2L, "192.168.1.2", 9050);
        backend3 = new Backend(3L, "192.168.1.3", 9050);
        backend4 = new Backend(4L, "192.168.1.4", 9050);
        backend5 = new Backend(5L, "192.168.1.5", 9050);
        backend6 = new Backend(6L, "192.168.1.6", 9050);
        // 7,8,9 are on same host
        backend7 = new Backend(7L, "192.168.1.8", 9050);
        backend8 = new Backend(8L, "192.168.1.8", 9050);
        backend9 = new Backend(9L, "192.168.1.8", 9050);

        backend1.setAlive(true);
        backend2.setAlive(true);
        backend3.setAlive(true);
        backend4.setAlive(true);
        backend5.setAlive(true);
        backend6.setAlive(true);
        backend7.setAlive(true);
        backend8.setAlive(true);
        backend9.setAlive(true);

        mixLoadScores = Maps.newHashMap();
        mixLoadScores.put(1L, 0.1);
        mixLoadScores.put(2L, 0.5);
        mixLoadScores.put(3L, 0.4);
        mixLoadScores.put(4L, 0.2);
        mixLoadScores.put(5L, 0.3);
        mixLoadScores.put(6L, 0.6);
        mixLoadScores.put(7L, 0.8);
        mixLoadScores.put(8L, 0.7);
        mixLoadScores.put(9L, 0.9);
    }

    private ColocateTableIndex createColocateIndex(GroupId groupId, List<Long> flatList) {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        int replicationNum = 3;
        List<List<Long>> backendsPerBucketSeq = Lists.partition(flatList, replicationNum);
        Map<Tag, List<List<Long>>> backendsPerBucketSeqMap = Maps.newHashMap();
        backendsPerBucketSeqMap.put(Tag.DEFAULT_BACKEND_TAG, backendsPerBucketSeq);
        colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeqMap);
        return colocateTableIndex;
    }

    private GlobalColocateStatistic createGlobalColocateStatistic(ColocateTableIndex colocateTableIndex,
            GroupId groupId) {
        GlobalColocateStatistic globalColocateStatistic = new GlobalColocateStatistic();
        List<Set<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeqSet(groupId);
        List<Long> totalReplicaDataSizes = Lists.newArrayList();
        for (int i = 0; i < backendsPerBucketSeq.size(); i++) {
            totalReplicaDataSizes.add(1L);
        }
        ReplicaAllocation replicaAlloc = new ReplicaAllocation((short) backendsPerBucketSeq.get(0).size());
        globalColocateStatistic.addGroup(groupId, replicaAlloc, backendsPerBucketSeq, totalReplicaDataSizes, 3);

        return globalColocateStatistic;
    }

    @Test
    public void testBalance(@Mocked SystemInfoService infoService,
            @Mocked LoadStatisticForTag statistic) {
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;
                infoService.getBackend(6L);
                result = backend6;
                minTimes = 0;
                infoService.getBackend(7L);
                result = backend7;
                minTimes = 0;
                infoService.getBackend(8L);
                result = backend8;
                minTimes = 0;
                infoService.getBackend(9L);
                result = backend9;
                minTimes = 0;

                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null);
                    }
                };
                minTimes = 0;
            }
        };
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", PrimitiveType.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5,
                ReplicaAllocation.DEFAULT_ALLOCATION);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // 1. balance a imbalance group
        // [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                Lists.newArrayList(1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L));
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        GlobalColocateStatistic globalColocateStatistic = createGlobalColocateStatistic(colocateTableIndex, groupId);
        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        boolean changed = (Boolean) Deencapsulation.invoke(balancer, "relocateAndBalance", groupId,
                Tag.DEFAULT_BACKEND_TAG, new HashSet<Long>(), allAvailBackendIds,
                colocateTableIndex, infoService, statistic, globalColocateStatistic,
                balancedBackendsPerBucketSeq, false);
        List<List<Long>> expected = Lists.partition(
                Lists.newArrayList(8L, 5L, 6L, 5L, 6L, 7L, 9L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L), 3);
        Assert.assertTrue("" + globalColocateStatistic, changed);
        Assert.assertEquals(expected, balancedBackendsPerBucketSeq);

        // 2. balance a already balanced group
        colocateTableIndex = createColocateIndex(groupId,
                Lists.newArrayList(9L, 8L, 7L, 8L, 6L, 5L, 9L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L));
        globalColocateStatistic = createGlobalColocateStatistic(colocateTableIndex, groupId);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        balancedBackendsPerBucketSeq.clear();
        changed = (Boolean) Deencapsulation.invoke(balancer, "relocateAndBalance", groupId,
                Tag.DEFAULT_BACKEND_TAG, new HashSet<Long>(), allAvailBackendIds,
                colocateTableIndex, infoService, statistic, globalColocateStatistic,
                balancedBackendsPerBucketSeq, false);
        System.out.println(balancedBackendsPerBucketSeq);
        Assert.assertFalse(changed);
        Assert.assertTrue(balancedBackendsPerBucketSeq.isEmpty());
    }

    @Test
    public void testFixBalanceEndlessLoop(@Mocked SystemInfoService infoService,
            @Mocked LoadStatisticForTag statistic) {
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = backend1;
                minTimes = 0;
                infoService.getBackend(2L);
                result = backend2;
                minTimes = 0;
                infoService.getBackend(3L);
                result = backend3;
                minTimes = 0;
                infoService.getBackend(4L);
                result = backend4;
                minTimes = 0;
                infoService.getBackend(5L);
                result = backend5;
                minTimes = 0;
                infoService.getBackend(6L);
                result = backend6;
                minTimes = 0;
                infoService.getBackend(7L);
                result = backend7;
                minTimes = 0;
                infoService.getBackend(8L);
                result = backend8;
                minTimes = 0;
                infoService.getBackend(9L);
                result = backend9;
                minTimes = 0;
                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null);
                    }
                };
                minTimes = 0;
            }
        };
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", PrimitiveType.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5,
                new ReplicaAllocation((short) 1));
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // 1. only one available backend
        // [[7], [7], [7], [7], [7]]
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId, Lists.newArrayList(7L, 7L, 7L, 7L, 7L));
        GlobalColocateStatistic globalColocateStatistic = createGlobalColocateStatistic(colocateTableIndex, groupId);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> allAvailBackendIds = Lists.newArrayList(7L);
        boolean changed = Deencapsulation.invoke(balancer, "relocateAndBalance", groupId, Tag.DEFAULT_BACKEND_TAG,
                new HashSet<Long>(), allAvailBackendIds, colocateTableIndex, infoService, statistic, globalColocateStatistic,
                balancedBackendsPerBucketSeq, false);
        Assert.assertFalse(changed);

        // 2. all backends are checked but this round is not changed
        // [[7], [7], [7], [7], [7]]
        // and add new backends 8, 9 that are on the same host with 7
        colocateTableIndex = createColocateIndex(groupId, Lists.newArrayList(7L, 7L, 7L, 7L, 7L));
        globalColocateStatistic = createGlobalColocateStatistic(colocateTableIndex, groupId);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        balancedBackendsPerBucketSeq = Lists.newArrayList();
        allAvailBackendIds = Lists.newArrayList(7L, 8L, 9L);
        changed = Deencapsulation.invoke(balancer, "relocateAndBalance", groupId, Tag.DEFAULT_BACKEND_TAG,
                new HashSet<Long>(), allAvailBackendIds, colocateTableIndex, infoService, statistic, globalColocateStatistic,
                balancedBackendsPerBucketSeq, false);
        Assert.assertFalse(changed);
    }

    @Test
    public void testFixBalanceEndlessLoop2(@Mocked SystemInfoService infoService,
            @Mocked LoadStatisticForTag statistic) {
        new Expectations() {
            {
                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null);
                    }
                };
                minTimes = 0;
            }
        };
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5, new ReplicaAllocation((short) 1));
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId, Lists.newArrayList(7L, 7L, 7L, 7L, 7L));
        GlobalColocateStatistic globalColocateStatistic = createGlobalColocateStatistic(colocateTableIndex, groupId);
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        Set<Long> unAvailBackendIds = Sets.newHashSet(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        List<Long> availBackendIds = Lists.newArrayList();
        boolean changed = (Boolean) Deencapsulation.invoke(balancer, "relocateAndBalance", groupId, Tag.DEFAULT_BACKEND_TAG,
                unAvailBackendIds, availBackendIds, colocateTableIndex, infoService, statistic, globalColocateStatistic,
                balancedBackendsPerBucketSeq, false);
        Assert.assertFalse(changed);
    }

    @Test
    public void testGetSortedBackendReplicaNumPairs(@Mocked LoadStatisticForTag statistic) {
        new Expectations() {
            {
                statistic.getBackendLoadStatistic(anyLong);
                result = new Delegate<BackendLoadStatistic>() {
                    BackendLoadStatistic delegate(Long beId) {
                        return new FakeBackendLoadStatistic(beId, null, null);
                    }
                };
                minTimes = 0;
            }
        };

        GlobalColocateStatistic globalColocateStatistic = new GlobalColocateStatistic();
        // all buckets are on different be
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        Set<Long> unavailBackendIds = Sets.newHashSet(9L);
        List<Long> flatBackendsPerBucketSeq = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        List<Map.Entry<Long, Long>> backends = Deencapsulation.invoke(balancer, "getSortedBackendReplicaNumPairs",
                allAvailBackendIds, unavailBackendIds, statistic, globalColocateStatistic, flatBackendsPerBucketSeq);
        long[] backendIds = backends.stream().mapToLong(Map.Entry::getKey).toArray();
        Assert.assertArrayEquals(new long[]{7L, 8L, 6L, 2L, 3L, 5L, 4L, 1L}, backendIds);

        // 0,1 bucket on same be and 5, 6 on same be
        flatBackendsPerBucketSeq = Lists.newArrayList(1L, 1L, 3L, 4L, 5L, 6L, 7L, 7L, 9L);
        backends = Deencapsulation.invoke(balancer, "getSortedBackendReplicaNumPairs", allAvailBackendIds, unavailBackendIds,
                statistic, globalColocateStatistic, flatBackendsPerBucketSeq);
        backendIds = backends.stream().mapToLong(Map.Entry::getKey).toArray();
        Assert.assertArrayEquals(new long[]{7L, 1L, 6L, 3L, 5L, 4L, 8L, 2L}, backendIds);
    }

    public final class FakeBackendLoadStatistic extends BackendLoadStatistic {
        public FakeBackendLoadStatistic(long beId, SystemInfoService infoService,
                TabletInvertedIndex invertedIndex) {
            super(beId, Tag.DEFAULT_BACKEND_TAG, infoService, invertedIndex);
        }

        @Override
        public double getMixLoadScore() {
            return mixLoadScores.get(getBeId());
        }

        @Override
        public BalanceStatus isFit(long tabletSize, TStorageMedium medium, List<RootPathLoadStatistic> result,
                boolean isSupplement) {
            return BalanceStatus.OK;
        }
    }

    @Test
    public void testGetBeSeqIndexes() {
        List<Long> flatBackendsPerBucketSeq = Lists.newArrayList(1L, 2L, 2L, 3L, 4L, 2L);
        List<Integer> indexes = Deencapsulation.invoke(balancer, "getBeSeqIndexes", flatBackendsPerBucketSeq, 2L);
        Assert.assertArrayEquals(new int[]{1, 2, 5}, indexes.stream().mapToInt(i -> i).toArray());
        System.out.println("backend1 id is " + backend1.getId());
    }

    @Test
    public void testGetUnavailableBeIdsInGroup(@Mocked ColocateTableIndex colocateTableIndex,
                                               @Mocked SystemInfoService infoService,
                                               @Mocked Backend myBackend2,
                                               @Mocked Backend myBackend3,
                                               @Mocked Backend myBackend4,
                                               @Mocked Backend myBackend5
    ) {
        GroupId groupId = new GroupId(10000, 10001);
        Tag tag = Tag.DEFAULT_BACKEND_TAG;
        Set<Long> allBackendsInGroup = Sets.newHashSet(1L, 2L, 3L, 4L, 5L);
        new Expectations() {
            {
                infoService.getBackend(1L);
                result = null;
                minTimes = 0;

                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isScheduleAvailable();
                result = true;
                minTimes = 0;
                myBackend2.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend2.isMixNode();
                result = true;
                minTimes = 0;

                // backend3 not available, and dead for a long time
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend3.isAlive();
                result = false;
                minTimes = 0;
                myBackend3.getLastUpdateMs();
                result = System.currentTimeMillis() - (Config.colocate_group_relocate_delay_second + 20) * 1000;
                minTimes = 0;
                myBackend3.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend3.isMixNode();
                result = true;
                minTimes = 0;

                // backend4 not available, and dead for a short time
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend4.isAlive();
                result = false;
                minTimes = 0;
                myBackend4.getLastUpdateMs();
                result = System.currentTimeMillis();
                minTimes = 0;
                myBackend4.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend4.isMixNode();
                result = true;
                minTimes = 0;

                // backend5 not available, and in decommission
                infoService.getBackend(5L);
                result = myBackend5;
                minTimes = 0;
                myBackend5.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend5.isAlive();
                result = true;
                minTimes = 0;
                myBackend5.isDecommissioned();
                result = true;
                minTimes = 0;
                myBackend5.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend5.isMixNode();
                result = true;
                minTimes = 0;

                colocateTableIndex.getBackendsByGroup(groupId, tag);
                result = allBackendsInGroup;
                minTimes = 0;
            }
        };

        Set<Long> unavailableBeIds = Deencapsulation.invoke(balancer, "getUnavailableBeIdsInGroup",
                infoService, colocateTableIndex, groupId, Tag.DEFAULT_BACKEND_TAG);
        System.out.println(unavailableBeIds);
        Assert.assertArrayEquals(new long[]{1L, 3L, 5L}, unavailableBeIds.stream().mapToLong(i -> i).sorted().toArray());
    }

    @Test
    public void testGetAvailableBeIds(@Mocked SystemInfoService infoService,
                                      @Mocked Backend myBackend2,
                                      @Mocked Backend myBackend3,
                                      @Mocked Backend myBackend4,
                                      @Mocked Backend myBackend5,
                                      @Mocked Backend myBackend6,
                                      @Mocked Backend myBackend7,
                                      @Mocked Backend myBackend8) throws AnalysisException {
        List<Long> clusterBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);
        new Expectations() {
            {
                infoService.getAllBackendIds(false);
                result = clusterBackendIds;
                minTimes = 0;

                infoService.getBackend(1L);
                result = null;
                minTimes = 0;

                // backend2 is available
                infoService.getBackend(2L);
                result = myBackend2;
                minTimes = 0;
                myBackend2.isScheduleAvailable();
                result = true;
                minTimes = 0;
                myBackend2.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend2.isMixNode();
                result = true;
                minTimes = 0;

                // backend3 not available, and dead for a long time
                infoService.getBackend(3L);
                result = myBackend3;
                minTimes = 0;
                myBackend3.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend3.isAlive();
                result = false;
                minTimes = 0;
                myBackend3.getLastUpdateMs();
                result = System.currentTimeMillis() - (Config.colocate_group_relocate_delay_second + 20) * 1000;
                minTimes = 0;
                myBackend3.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend3.isMixNode();
                result = true;
                minTimes = 0;

                // backend4 available, not alive but dead for a short time
                infoService.getBackend(4L);
                result = myBackend4;
                minTimes = 0;
                myBackend4.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend4.isAlive();
                result = false;
                minTimes = 0;
                myBackend4.getLastUpdateMs();
                result = System.currentTimeMillis();
                minTimes = 0;
                myBackend4.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend4.isMixNode();
                result = true;
                minTimes = 0;

                // backend5 not available, and in decommission
                infoService.getBackend(5L);
                result = myBackend5;
                minTimes = 0;
                myBackend5.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend5.isAlive();
                result = true;
                minTimes = 0;
                myBackend5.isDecommissioned();
                result = true;
                minTimes = 0;
                myBackend5.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend5.isMixNode();
                result = true;
                minTimes = 0;

                // backend6 is available, but with different tag
                infoService.getBackend(5L);
                result = myBackend6;
                minTimes = 0;
                myBackend6.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend6.isAlive();
                result = true;
                minTimes = 0;
                myBackend6.isDecommissioned();
                result = false;
                minTimes = 0;
                myBackend6.getLocationTag();
                result = Tag.create(Tag.TYPE_LOCATION, "new_loc");
                minTimes = 0;
                myBackend6.isMixNode();
                result = true;
                minTimes = 0;

                // backend7 is available, but in exclude sets
                infoService.getBackend(5L);
                result = myBackend7;
                minTimes = 0;
                myBackend7.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend7.isAlive();
                result = true;
                minTimes = 0;
                myBackend7.isDecommissioned();
                result = false;
                minTimes = 0;
                myBackend7.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend7.isMixNode();
                result = true;
                minTimes = 0;
                myBackend7.getId();
                result = 999L;
                minTimes = 0;

                // backend8 is available, it's a compute node.
                infoService.getBackend(5L);
                result = myBackend8;
                minTimes = 0;
                myBackend8.isScheduleAvailable();
                result = false;
                minTimes = 0;
                myBackend8.isAlive();
                result = true;
                minTimes = 0;
                myBackend8.isDecommissioned();
                result = false;
                minTimes = 0;
                myBackend8.getLocationTag();
                result = Tag.DEFAULT_BACKEND_TAG;
                minTimes = 0;
                myBackend8.isMixNode();
                result = false;
                minTimes = 0;
            }
        };

        List<Long> availableBeIds = Deencapsulation.invoke(balancer, "getAvailableBeIds",
                Tag.DEFAULT_BACKEND_TAG, Sets.newHashSet(999L), infoService);
        System.out.println(availableBeIds);
        Assert.assertArrayEquals(new long[]{2L, 4L}, availableBeIds.stream().mapToLong(i -> i).sorted().toArray());
    }

    @Test
    public void testGlobalColocateStatistic() {
        GroupId groupId1 = new GroupId(1L, 10000L);
        GroupId groupId2 = new GroupId(2L, 20000L);
        GlobalColocateStatistic globalColocateStatistic = new GlobalColocateStatistic();
        globalColocateStatistic.addGroup(groupId1, new ReplicaAllocation((short) 2),
                Lists.newArrayList(Sets.newHashSet(1001L, 1002L), Sets.newHashSet(1002L, 1003L),
                        Sets.newHashSet(1001L, 1003L)),
                Lists.newArrayList(100L, 200L, 300L), 5);
        globalColocateStatistic.addGroup(groupId2, new ReplicaAllocation((short) 1),
                Lists.newArrayList(Sets.newHashSet(1001L), Sets.newHashSet(1002L),
                        Sets.newHashSet(1003L), Sets.newHashSet(1001L)),
                Lists.newArrayList(100L, 200L, 300L, 400L), 7);

        Map<Long, BackendBuckets> backendBucketsMap = globalColocateStatistic.getBackendBucketsMap();
        BackendBuckets backendBuckets1 = backendBucketsMap.get(1001L);
        Assert.assertNotNull(backendBuckets1);
        Assert.assertEquals(Lists.newArrayList(0, 2),
                backendBuckets1.getGroupTabletOrderIndices().get(groupId1));
        Assert.assertEquals(Lists.newArrayList(0, 3),
                backendBuckets1.getGroupTabletOrderIndices().get(groupId2));
        BackendBuckets backendBuckets2 = backendBucketsMap.get(1002L);
        Assert.assertNotNull(backendBuckets2);
        Assert.assertEquals(Lists.newArrayList(0, 1),
                backendBuckets2.getGroupTabletOrderIndices().get(groupId1));
        Assert.assertEquals(Lists.newArrayList(1),
                backendBuckets2.getGroupTabletOrderIndices().get(groupId2));
        BackendBuckets backendBuckets3 = backendBucketsMap.get(1003L);
        Assert.assertNotNull(backendBuckets3);
        Assert.assertEquals(Lists.newArrayList(1, 2),
                backendBuckets3.getGroupTabletOrderIndices().get(groupId1));
        Assert.assertEquals(Lists.newArrayList(2),
                backendBuckets3.getGroupTabletOrderIndices().get(groupId2));

        Map<GroupId, List<BucketStatistic>> allGroupBucketsMap = globalColocateStatistic.getAllGroupBucketsMap();
        Assert.assertEquals(Lists.newArrayList(new BucketStatistic(0, 5, 100L), new BucketStatistic(1, 5, 200L),
                    new BucketStatistic(2, 5, 300L)),
                allGroupBucketsMap.get(groupId1));
        Assert.assertEquals(Lists.newArrayList(new BucketStatistic(0, 7, 100L), new BucketStatistic(1, 7, 200L),
                    new BucketStatistic(2, 7, 300L), new BucketStatistic(3, 7, 400L)),
                allGroupBucketsMap.get(groupId2));

        Map<Tag, Integer> expectAllTagBucketNum = Maps.newHashMap();
        expectAllTagBucketNum.put(Tag.DEFAULT_BACKEND_TAG, 10);
        Assert.assertEquals(expectAllTagBucketNum, globalColocateStatistic.getAllTagBucketNum());
    }
}
