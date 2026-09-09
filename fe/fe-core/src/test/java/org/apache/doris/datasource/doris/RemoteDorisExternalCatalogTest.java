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

import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RemoteDorisExternalCatalogTest {

    private final Set<Long> localBackendIds = new HashSet<>(Arrays.asList(1L, 2L));

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
        Assertions.assertFalse(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.singletonList(remoteTable), localBackendIds));
    }

    @Test
    public void testConflictWithLocalBackend() {
        // remote backend id 1 collides with local backend id 1
        RemoteOlapTable remoteTable = remoteTableWithBackends(ImmutableMap.of(
                100L, new Backend(100L, "10.1.1.1", 9050),
                1L, new Backend(1L, "10.1.1.2", 9050)));
        Assertions.assertTrue(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.singletonList(remoteTable), localBackendIds));
    }

    @Test
    public void testConflictBetweenRemoteTables() {
        // two remote catalogs independently allocate backend id 200
        RemoteOlapTable remoteTableA = remoteTableWithBackends(ImmutableMap.of(
                200L, new Backend(200L, "10.1.1.1", 9050)));
        RemoteOlapTable remoteTableB = remoteTableWithBackends(ImmutableMap.of(
                200L, new Backend(200L, "10.2.1.1", 9050)));
        List<RemoteOlapTable> remoteTables = Arrays.asList(remoteTableA, remoteTableB);
        Assertions.assertTrue(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(remoteTables, localBackendIds));
    }

    @Test
    public void testNoRemoteTable() {
        Assertions.assertFalse(RemoteDorisExternalCatalog
                .hasRemoteBackendIdConflict(Collections.emptyList(), localBackendIds));
    }
}
