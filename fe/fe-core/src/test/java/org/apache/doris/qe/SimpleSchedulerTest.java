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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleSchedulerTest {

    private static Backend be1;
    private static Backend be2;
    private static Backend be3;
    private static Backend be4;
    private static Backend be5;

    @BeforeClass
    public static void setUp() {
        SimpleScheduler.init();
        Config.heartbeat_interval_second = 2;
        be1 = new Backend(1000L, "192.168.100.0", 9050);
        be2 = new Backend(1001L, "192.168.100.1", 9050);
        be3 = new Backend(1002L, "192.168.100.2", 9050);
        be4 = new Backend(1003L, "192.168.100.3", 9050);
        be5 = new Backend(1004L, "192.168.100.4", 9050);
        be1.setAlive(true);
        be2.setAlive(true);
        be3.setAlive(true);
        be4.setAlive(true);
        be5.setAlive(true);
    }

    private static Map<Long, Backend> genBackends() {
        Map<Long, Backend> map = Maps.newHashMap();
        map.put(be1.getId(), be1);
        map.put(be2.getId(), be2);
        map.put(be3.getId(), be3);
        map.put(be4.getId(), be4);
        map.put(be5.getId(), be5);
        return map;
    }

    @Test
    public void testGetHostNormal() throws UserException, InterruptedException {
        Reference<Long> ref = new Reference<Long>();
        ImmutableMap<Long, Backend> backends = ImmutableMap.copyOf(genBackends());

        List<TScanRangeLocation> locations = Lists.newArrayList();
        TScanRangeLocation scanRangeLocation1 = new TScanRangeLocation();
        scanRangeLocation1.setBackendId(be1.getId());
        locations.add(scanRangeLocation1);
        TScanRangeLocation scanRangeLocation2 = new TScanRangeLocation();
        scanRangeLocation2.setBackendId(be2.getId());
        locations.add(scanRangeLocation2);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean foundCandidate = false;
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < 1000; i++) {
                        TNetworkAddress address = SimpleScheduler.getHost(locations.get(0).backend_id, locations, backends, ref);
                        Assert.assertNotNull(address);
                        if (!foundCandidate && address.getHostname().equals(be2.getHost())) {
                            foundCandidate = true;
                        }
                    }
                    System.out.println("cost: " + (System.currentTimeMillis() - start));
                    Assert.assertTrue(foundCandidate);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Set<String> resBackends = Sets.newHashSet();
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < 1000; i++) {
                        TNetworkAddress address = SimpleScheduler.getHost(backends, ref);
                        Assert.assertNotNull(address);
                        resBackends.add(address.hostname);
                    }
                    System.out.println("cost: " + (System.currentTimeMillis() - start));
                    Assert.assertTrue(resBackends.size() >= 4);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                SimpleScheduler.addToBlacklist(be1.getId(), "test");
            }
        });

        t3.start();
        t1.start();
        t2.start();

        t1.join();
        t2.join();
        t3.join();

        Assert.assertFalse(SimpleScheduler.isAvailable(be1));
        Thread.sleep((Config.heartbeat_interval_second + 5) * 1000);
        Assert.assertTrue(SimpleScheduler.isAvailable(be1));
    }

    @Test
    public void testGetHostAbnormal() throws UserException, InterruptedException {
        Reference<Long> ref = new Reference<Long>();
        ImmutableMap<Long, Backend> backends = ImmutableMap.copyOf(genBackends());

        // 1. unknown backends
        List<TScanRangeLocation> locations = Lists.newArrayList();
        TScanRangeLocation scanRangeLocation1 = new TScanRangeLocation();
        scanRangeLocation1.setBackendId(2000L);
        locations.add(scanRangeLocation1);
        TScanRangeLocation scanRangeLocation2 = new TScanRangeLocation();
        scanRangeLocation2.setBackendId(2001L);
        locations.add(scanRangeLocation2);

        try {
            SimpleScheduler.getHost(locations.get(0).backend_id, locations, backends, ref);
            Assert.fail();
        } catch (UserException e) {
            System.out.println(e.getMessage());
        }

        // 2. all backends in black list
        locations.clear();
        scanRangeLocation1 = new TScanRangeLocation();
        scanRangeLocation1.setBackendId(be1.getId());
        locations.add(scanRangeLocation1);
        scanRangeLocation2 = new TScanRangeLocation();
        scanRangeLocation2.setBackendId(be2.getId());
        locations.add(scanRangeLocation2);
        TScanRangeLocation scanRangeLocation3 = new TScanRangeLocation();
        scanRangeLocation3.setBackendId(be3.getId());
        locations.add(scanRangeLocation3);
        TScanRangeLocation scanRangeLocation4 = new TScanRangeLocation();
        scanRangeLocation4.setBackendId(be4.getId());
        locations.add(scanRangeLocation4);
        TScanRangeLocation scanRangeLocation5 = new TScanRangeLocation();
        scanRangeLocation5.setBackendId(be5.getId());
        locations.add(scanRangeLocation5);

        SimpleScheduler.addToBlacklist(be1.getId(), "test");
        SimpleScheduler.addToBlacklist(be2.getId(), "test");
        SimpleScheduler.addToBlacklist(be3.getId(), "test");
        SimpleScheduler.addToBlacklist(be4.getId(), "test");
        SimpleScheduler.addToBlacklist(be5.getId(), "test");
        try {
            SimpleScheduler.getHost(locations.get(0).backend_id, locations, backends, ref);
            Assert.fail();
        } catch (UserException e) {
            System.out.println(e.getMessage());
        }

        Thread.sleep((Config.heartbeat_interval_second + 5) * 1000);
        Assert.assertNotNull(SimpleScheduler.getHost(locations.get(0).backend_id, locations, backends, ref));
    }
}
