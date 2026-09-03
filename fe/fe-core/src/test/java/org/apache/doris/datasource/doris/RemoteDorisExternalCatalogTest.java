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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Env;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RemoteDorisExternalCatalogTest {

    private Backend localBackend1;
    private Backend localBackend2;

    @Before
    public void setUp() {
        localBackend1 = new Backend(1L, "192.168.1.1", 9050);
        localBackend2 = new Backend(2L, "192.168.1.2", 9050);
        Env.getCurrentSystemInfo().addBackend(localBackend1);
        Env.getCurrentSystemInfo().addBackend(localBackend2);
    }

    @After
    public void tearDown() throws Exception {
        Env.getCurrentSystemInfo().dropBackend(localBackend1.getId());
        Env.getCurrentSystemInfo().dropBackend(localBackend2.getId());
    }

    private RemoteOlapTable remoteTableWithBackends(Map<Long, Backend> backends) {
        return new RemoteOlapTable() {
            @Override
            public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() {
                return ImmutableMap.copyOf(backends);
            }
        };
    }

    @Test
    public void testNoConflict() {
        RemoteOlapTable remoteTable = remoteTableWithBackends(ImmutableMap.of(
                100L, new Backend(100L, "10.1.1.1", 9050),
                101L, new Backend(101L, "10.1.1.2", 9050)));
        Assert.assertFalse(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.singletonList(remoteTable)));
    }

    @Test
    public void testConflictWithLocalBackend() {
        // remote backend id 1 collides with local backend id 1
        RemoteOlapTable remoteTable = remoteTableWithBackends(ImmutableMap.of(
                100L, new Backend(100L, "10.1.1.1", 9050),
                1L, new Backend(1L, "10.1.1.2", 9050)));
        Assert.assertTrue(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.singletonList(remoteTable)));
    }

    @Test
    public void testConflictBetweenRemoteTables() {
        // two remote catalogs independently allocate backend id 200
        RemoteOlapTable remoteTableA = remoteTableWithBackends(ImmutableMap.of(
                200L, new Backend(200L, "10.1.1.1", 9050)));
        RemoteOlapTable remoteTableB = remoteTableWithBackends(ImmutableMap.of(
                200L, new Backend(200L, "10.2.1.1", 9050)));
        List<RemoteOlapTable> remoteTables = Arrays.asList(remoteTableA, remoteTableB);
        Assert.assertTrue(RemoteDorisExternalCatalog.hasRemoteBackendIdConflict(remoteTables));
    }

    @Test
    public void testNoRemoteTable() {
        Assert.assertFalse(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.emptyList()));
    }
}
