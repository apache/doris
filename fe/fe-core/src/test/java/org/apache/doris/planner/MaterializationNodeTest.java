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

package org.apache.doris.planner;

import org.apache.doris.common.Config;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNodeInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MaterializationNodeTest {

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
            info.tryAddBlackList("MaterializationNodeTest");
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
            List<Backend> localBackends = Arrays.asList(localBlacklisted, localHealthy);
            List<Backend> remoteBackends = Arrays.asList(remoteHealthy);
            List<TNodeInfo> nodes = MaterializationNode.buildNodesInfo(localBackends, remoteBackends).getNodes();

            List<String> hosts = nodes.stream().map(TNodeInfo::getHost).toList();
            Assertions.assertEquals(2, nodes.size());
            // Local blacklisted backend 100 is filtered out by the policy.
            Assertions.assertFalse(hosts.contains("192.168.0.100"));
            // Local healthy backend is kept.
            Assertions.assertTrue(hosts.contains("192.168.0.200"));
            // Remote backend 100 survives the local blacklist and is the only id=100 entry.
            Assertions.assertTrue(hosts.contains("10.0.0.100"));
            Assertions.assertEquals(1, nodes.stream().filter(node -> node.getId() == 100).count());
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

        List<TNodeInfo> nodes = MaterializationNode.buildNodesInfo(Arrays.asList(localHealthy),
                Arrays.asList(remoteDead, remoteHealthy)).getNodes();

        List<String> hosts = nodes.stream().map(TNodeInfo::getHost).toList();
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertFalse(hosts.contains("10.0.0.100"));
        Assertions.assertTrue(hosts.contains("10.0.0.300"));
        Assertions.assertTrue(hosts.contains("192.168.0.200"));
    }
}
