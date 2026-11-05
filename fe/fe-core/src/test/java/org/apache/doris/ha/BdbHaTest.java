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

package org.apache.doris.ha;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.bdbje.BDBEnvironment;

import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.ReplicationSSLConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.net.SSLChannelFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

public class BdbHaTest {
    private static final String MASTER = "test-master";
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("127.0.0.1", 9010);

    private final List<ReplicationGroupAdmin> admins = new ArrayList<>();
    private final List<ScheduledExecutorService> monitors = new ArrayList<>();
    private BDBHA ha;
    private boolean fail;
    private String keyStore;

    enum Operation {
        GET_OBSERVER_NODES,
        GET_ELECTABLE_NODES,
        GET_ELECTABLE_NODES_WITHOUT_LEADER,
        GET_LEADER,
        REMOVE_ELECTABLE_NODE,
        UPDATE_NODE_ADDRESS,
        REMOVE_CONFLICT_NODE,
        REMOVE_DROPPED_MEMBER
    }

    @BeforeEach
    public void setUp() throws Exception {
        keyStore = System.getProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStore");
        Path certificates = Paths.get(System.getenv().getOrDefault("DORIS_HOME", "../.."),
                "regression-test", "ssl_default_certificate");
        ReplicationSSLConfig config = new ReplicationSSLConfig();
        config.setSSLPemCaCertFile(certificates.resolve("ca.pem").toString());
        config.setSSLPemCertFile(certificates.resolve("client-cert.pem").toString());
        // PKCS#8 form of the existing regression-test client key, as required by JE.
        config.setSSLPemKeyFile(certificates.resolve("client-key-pkcs8.pem").toString());
        config.setSSLCertRefreshIntervalSeconds(3600);

        ReplicationNode node = Mockito.mock(ReplicationNode.class);
        Mockito.when(node.getName()).thenReturn(MASTER);
        Mockito.when(node.getHostName()).thenReturn(ADDRESS.getHostString());
        Mockito.when(node.getPort()).thenReturn(ADDRESS.getPort());
        Mockito.when(node.getSocketAddress()).thenReturn(ADDRESS);
        ReplicationGroup group = Mockito.mock(ReplicationGroup.class);
        Mockito.when(group.getMember(MASTER)).thenReturn(node);
        Mockito.when(group.getElectableNodes()).thenReturn(Collections.singleton(node));
        Mockito.when(group.getSecondaryNodes()).thenReturn(Collections.singleton(node));

        BDBEnvironment environment = Mockito.mock(BDBEnvironment.class);
        Mockito.when(environment.getReplicationGroupAdmin()).thenAnswer(invocation -> {
            // Keep real JE construction, SSL factories, schedulers and close().
            // Only replace RPCs, so these tests need no running replication group.
            ReplicationGroupAdmin admin = Mockito.spy(new ReplicationGroupAdmin("test-group",
                    Collections.singleton(ADDRESS), config));
            Mockito.doAnswer(call -> {
                if (fail) {
                    throw new UnknownMasterException("test failure");
                }
                return group;
            }).when(admin).getGroup();
            Mockito.doReturn(MASTER).when(admin).getMasterNodeName();
            Mockito.doAnswer(call -> {
                if (fail) {
                    throw new MemberNotFoundException("test failure");
                }
                return null;
            }).when(admin).removeMember(Mockito.anyString());
            Mockito.doAnswer(call -> {
                if (fail) {
                    throw new MemberNotFoundException("test failure");
                }
                return null;
            }).when(admin).updateAddress(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
            admins.add(admin);
            SSLChannelFactory factory = Deencapsulation.getField(admin, "channelFactory");
            ScheduledExecutorService monitor = Deencapsulation.getField(factory, "certificateCheckExecutor");
            Assertions.assertNotNull(monitor, "The test must enable real PEM certificate monitoring");
            monitors.add(monitor);
            return admin;
        });
        ha = new BDBHA(environment, MASTER);
    }

    @AfterEach
    public void tearDown() {
        // Also clean up when run against the unfixed implementation.
        for (ReplicationGroupAdmin admin : admins) {
            admin.close();
        }
        if (keyStore == null) {
            System.clearProperty("javax.net.ssl.keyStore");
        } else {
            System.setProperty("javax.net.ssl.keyStore", keyStore);
        }
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    public void testReleasesCertificateMonitor(Operation operation) {
        invoke(operation);
        assertMonitorsReleased();
    }

    @ParameterizedTest
    @EnumSource(Operation.class)
    public void testReleasesCertificateMonitorOnFailure(Operation operation) {
        fail = true;
        switch (operation) {
            case GET_LEADER:
            case REMOVE_CONFLICT_NODE:
            case REMOVE_DROPPED_MEMBER:
                Assertions.assertThrows(UnknownMasterException.class, () -> invoke(operation));
                break;
            default:
                invoke(operation);
        }
        assertMonitorsReleased();
    }

    @Test
    public void testRepeatedFrontendQueriesDoNotAccumulateMonitors() {
        for (int i = 0; i < 40; i++) {
            ha.getLeader();
            ha.getElectableNodes(true);
            ha.getObserverNodes();
        }
        Assertions.assertEquals(120, monitors.size());
        assertMonitorsReleased();
    }

    @Test
    public void testNoAvailableAdmin() {
        BDBHA empty = new BDBHA(Mockito.mock(BDBEnvironment.class), MASTER);
        Assertions.assertTrue(empty.getObserverNodes().isEmpty());
        Assertions.assertTrue(empty.getElectableNodes(true).isEmpty());
        Assertions.assertThrows(NullPointerException.class, empty::getLeader);
        Assertions.assertFalse(empty.removeElectableNode(MASTER));
        Assertions.assertFalse(empty.updateNodeAddress(MASTER, "127.0.0.2", 9010));
        empty.removeConflictNodeIfExist(ADDRESS.getHostString(), ADDRESS.getPort());
        empty.removeDroppedMember(new ConcurrentLinkedQueue<>());
        Assertions.assertTrue(admins.isEmpty());
    }

    private void assertMonitorsReleased() {
        Assertions.assertFalse(monitors.isEmpty());
        for (ScheduledExecutorService monitor : monitors) {
            Assertions.assertTrue(monitor.isTerminated(), "BDB SSL certificate monitor was not released");
        }
    }

    private void invoke(Operation operation) {
        switch (operation) {
            case GET_OBSERVER_NODES:
                Assertions.assertEquals(fail ? Collections.emptyList() : Collections.singletonList(ADDRESS),
                        ha.getObserverNodes());
                break;
            case GET_ELECTABLE_NODES:
                Assertions.assertEquals(fail ? Collections.emptyList() : Collections.singletonList(ADDRESS),
                        ha.getElectableNodes(true));
                break;
            case GET_ELECTABLE_NODES_WITHOUT_LEADER:
                Assertions.assertTrue(ha.getElectableNodes(false).isEmpty());
                break;
            case GET_LEADER:
                Assertions.assertEquals(ADDRESS, ha.getLeader());
                break;
            case REMOVE_ELECTABLE_NODE:
                Assertions.assertEquals(!fail, ha.removeElectableNode(MASTER));
                break;
            case UPDATE_NODE_ADDRESS:
                Assertions.assertEquals(!fail, ha.updateNodeAddress(MASTER, "127.0.0.2", 9010));
                break;
            case REMOVE_CONFLICT_NODE:
                ha.removeConflictNodeIfExist(ADDRESS.getHostString(), ADDRESS.getPort());
                break;
            case REMOVE_DROPPED_MEMBER:
                ha.removeDroppedMember(new ConcurrentLinkedQueue<>(Collections.singleton(MASTER)));
                break;
            default:
                throw new AssertionError(operation);
        }
    }
}
