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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import mockit.Expectations;
import mockit.Mocked;

public class ColocateTableBalancerTest {
    
    @Mocked
    private Catalog catalog;
    @Mocked
    private SystemInfoService infoService;

    private TabletScheduler tabletScheduler;
    
    private ColocateTableBalancer balancer = ColocateTableBalancer.getInstance();
    
    private Backend backend1;
    private Backend backend2;
    private Backend backend3;
    private Backend backend4;
    private Backend backend5;
    private Backend backend6;
    private Backend backend7;
    private Backend backend8;
    private Backend backend9;

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
            }
        };
    }

    private ColocateTableIndex createColocateIndex(GroupId groupId, List<Long> flatList) {
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();
        int replicationNum = 3;
        List<List<Long>> backendsPerBucketSeq = Lists.partition(flatList, replicationNum);
        colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
        return colocateTableIndex;
    }

    @Test
    public void testBalance() {
        GroupId groupId = new GroupId(10000, 10001);
        List<Column> distributionCols = Lists.newArrayList();
        distributionCols.add(new Column("k1", PrimitiveType.INT));
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, distributionCols, 5, (short) 3);
        Map<GroupId, ColocateGroupSchema> group2Schema = Maps.newHashMap();
        group2Schema.put(groupId, groupSchema);

        // 1. balance a imbalance group
        // [[1, 2, 3], [4, 1, 2], [3, 4, 1], [2, 3, 4], [1, 2, 3]]
        ColocateTableIndex colocateTableIndex = createColocateIndex(groupId,
                Lists.newArrayList(1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L));
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);

        List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
        List<Long> allAvailBackendIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        boolean changed = (Boolean) Deencapsulation.invoke(balancer, "balance", groupId, allAvailBackendIds,
                colocateTableIndex, infoService, balancedBackendsPerBucketSeq);
        System.out.println(balancedBackendsPerBucketSeq);
        List<List<Long>> expected = Lists.partition(
                Lists.newArrayList(9L, 5L, 3L, 4L, 6L, 8L, 7L, 6L, 1L, 2L, 9L, 4L, 1L, 2L, 3L), 3);
        Assert.assertTrue(changed);
        Assert.assertEquals(expected, balancedBackendsPerBucketSeq);

        // 2. balance a already balanced group
        colocateTableIndex = createColocateIndex(groupId,
                Lists.newArrayList(9L, 8L, 7L, 8L, 6L, 5L, 9L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L));
        Deencapsulation.setField(colocateTableIndex, "group2Schema", group2Schema);
        balancedBackendsPerBucketSeq.clear();
        changed = (Boolean) Deencapsulation.invoke(balancer, "balance", groupId, allAvailBackendIds,
                colocateTableIndex, infoService, balancedBackendsPerBucketSeq);
        System.out.println(balancedBackendsPerBucketSeq);
        Assert.assertFalse(changed);
        Assert.assertTrue(balancedBackendsPerBucketSeq.isEmpty());
    }
}
