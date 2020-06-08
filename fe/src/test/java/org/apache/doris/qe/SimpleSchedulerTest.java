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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Reference;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleSchedulerTest {
    static Reference<Long> ref = new Reference<Long>();

    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() {
        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
    }

    // TODO(lingbin): PALO-2051.
    // Comment out these code temporatily.
    // @Test
    public void testGetHostWithBackendId() {
        FeConstants.heartbeat_interval_second = Integer.MAX_VALUE;
        TNetworkAddress address;
        // three locations
        List<TScanRangeLocation> nullLocations = null;
        List<TScanRangeLocation> emptyLocations = new ArrayList<TScanRangeLocation>();

        List<TScanRangeLocation> twoLocations = new ArrayList<TScanRangeLocation>();
        TScanRangeLocation locationA = new TScanRangeLocation();
        TScanRangeLocation locationB = new TScanRangeLocation();
        locationA.setBackend_id(20);
        locationA.setBackend_id(30);
        twoLocations.add(locationA);
        twoLocations.add(locationB);

        // three Backends
        ImmutableMap<Long, Backend> nullBackends = null;
        ImmutableMap<Long, Backend> emptyBackends = ImmutableMap.of();

        Backend backendA = new Backend(0, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2, "addressC", 0);
        backendC.updateOnce(0, 0, 0);

        Map<Long, Backend> threeBackends = Maps.newHashMap();
        threeBackends.put((long) 0, backendA);
        threeBackends.put((long) 1, backendB);
        threeBackends.put((long) 2, backendC);
        ImmutableMap<Long, Backend> immutableThreeBackends = ImmutableMap.copyOf(threeBackends);

        {   // null Backends
            address = SimpleScheduler.getHost(Long.valueOf(0), nullLocations,
                                              nullBackends, ref);
            Assert.assertNull(address);
        }
        {   // empty Backends
            address = SimpleScheduler.getHost(Long.valueOf(0), emptyLocations,
                                              emptyBackends, ref);
            Assert.assertNull(address);
        }
        {   // normal Backends

            // BackendId exists
            Assert.assertEquals(SimpleScheduler.getHost(0, emptyLocations, immutableThreeBackends, ref)
                    .hostname, "addressA");
            Assert.assertEquals(SimpleScheduler.getHost(2, emptyLocations, immutableThreeBackends, ref)
                    .hostname, "addressC");

            // BacknedId not exists and location exists, choose the locations's first
            Assert.assertEquals(SimpleScheduler.getHost(3, twoLocations, immutableThreeBackends, ref)
                    .hostname, "addressA");
        }
        {   // abnormal
            // BackendId not exists and location not exists
            Assert.assertNull(SimpleScheduler.getHost(3, emptyLocations, immutableThreeBackends, ref));
        }

    }

    // TODO(lingbin): PALO-2051.
    // Comment out these code temporatily.
    // @Test
    public void testGetHostWithNoParams() {
        FeConstants.heartbeat_interval_second = Integer.MAX_VALUE;
        ImmutableMap<Long, Backend> nullBackends = null;
        ImmutableMap<Long, Backend> emptyBackends = ImmutableMap.of();

        Backend backendA = new Backend(0, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2, "addressC", 0);
        backendC.updateOnce(0, 0, 0);
        Map<Long, Backend> threeBackends = Maps.newHashMap();
        threeBackends.put((long) 0, backendA);
        threeBackends.put((long) 1, backendB);
        threeBackends.put((long) 2, backendC);
        ImmutableMap<Long, Backend> immutableThreeBackends = ImmutableMap.copyOf(threeBackends);

        {   // abmormal
            Assert.assertNull(SimpleScheduler.getHost(nullBackends, ref));
            Assert.assertNull(SimpleScheduler.getHost(emptyBackends, ref));
        }   // normal
        {
            String a = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            String b = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            String c = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            Assert.assertTrue(!a.equals(b) && !a.equals(c) && !b.equals(c));
            a = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            b = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            c = SimpleScheduler.getHost(immutableThreeBackends, ref).hostname;
            Assert.assertTrue(!a.equals(b) && !a.equals(c) && !b.equals(c));
        }
    }

    // TODO(lingbin): PALO-2051.
    // Comment out these code temporatily.
    // @Test
    public void testBlackList() {
        FeConstants.heartbeat_interval_second = Integer.MAX_VALUE;
        TNetworkAddress address = null;

        Backend backendA = new Backend(0, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2, "addressC", 0);
        backendC.updateOnce(0, 0, 0);
        Map<Long, Backend> threeBackends = Maps.newHashMap();
        threeBackends.put((long) 100, backendA);
        threeBackends.put((long) 101, backendB);
        threeBackends.put((long) 102, backendC);
        ImmutableMap<Long, Backend> immutableThreeBackends = ImmutableMap.copyOf(threeBackends);

        SimpleScheduler.addToBlacklist(Long.valueOf(100));
        SimpleScheduler.addToBlacklist(Long.valueOf(101));
        address = SimpleScheduler.getHost(immutableThreeBackends, ref);
        // only backendc can work
        Assert.assertEquals(address.hostname, "addressC");
        SimpleScheduler.addToBlacklist(Long.valueOf(102));
        // no backend can work
        address = SimpleScheduler.getHost(immutableThreeBackends, ref);
        Assert.assertNull(address);
    }
}
