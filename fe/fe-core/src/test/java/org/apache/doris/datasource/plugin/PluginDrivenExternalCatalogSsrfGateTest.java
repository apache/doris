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

package org.apache.doris.datasource.plugin;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.DdlException;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSecuritySurface;
import org.apache.doris.connector.spi.ConnectorSession;

import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CREATE / ALTER CATALOG must apply the operator's SSRF policy to every user-supplied endpoint the FE would
 * connect to, rejecting an intranet target. This mirrors {@link PluginDrivenExternalCatalogDriverUrlGateTest}:
 * the judgment is delegated to the third-party {@code SecurityChecker} SDK (absent in CI, so a recording
 * fake stands in), the checked endpoint keys are the ones the connector declares through
 * {@code ConnectorSecuritySurface} (injected here via the {@code securitySurface} seam), the
 * gate runs on CREATE ({@code checkWhenCreating}) and the {@code !isReplay} ALTER path
 * ({@code validatePropertiesBeforeUpdate}) but never on replay, and it is a no-op when no checker is
 * installed.
 */
public class PluginDrivenExternalCatalogSsrfGateTest {

    private RecordingChecker checker;

    @BeforeEach
    public void setUp() {
        checker = new RecordingChecker();
        SecurityChecker.setInstanceForTest(checker);
        // A fresh empty manager so the connector's own property validation is a no-op and the SSRF gate is
        // tested in isolation, regardless of what a prior test in a reused fork left behind.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @AfterEach
    public void tearDown() {
        // Revert to the Config-driven singleton so a reused surefire fork is not left with the fake.
        SecurityChecker.setInstanceForTest(null);
        // An empty manager, not null: a null singleton would make later tests' ConnectorFactory calls
        // silently no-op instead of fail (same shape as the sibling driver-url gate test's tearDown).
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    // ---- static helper: endpoint parsing ----

    @Test
    public void parseEndpointExtractsHostAndPort() {
        HostAndPort schemed = PluginDrivenExternalCatalog.parseEndpoint("thrift://host1:9083");
        Assertions.assertNotNull(schemed);
        Assertions.assertEquals("host1", schemed.getHost());
        Assertions.assertEquals(9083, schemed.getPort());

        HostAndPort bare = PluginDrivenExternalCatalog.parseEndpoint("host2:9083");
        Assertions.assertNotNull(bare);
        Assertions.assertEquals("host2", bare.getHost());
        Assertions.assertEquals(9083, bare.getPort());
    }

    @Test
    public void parseEndpointReturnsNullWhenNoHostPort() {
        Assertions.assertNull(PluginDrivenExternalCatalog.parseEndpoint(""));
        Assertions.assertNull(PluginDrivenExternalCatalog.parseEndpoint("thrift://host-without-port"));
        Assertions.assertNull(PluginDrivenExternalCatalog.parseEndpoint("thrift://"));
    }

    // ---- ALTER gate ----

    @Test
    public void alterRejectsRepointingMetastoreToIntranet() {
        checker.intranetHost = "10.0.0.1";
        TestCatalog catalog = new TestCatalog(hms("thrift://hms.public.example.com:9083"));

        // MUTATION: drop checkEndpointsAgainstSsrf from validatePropertiesBeforeUpdate -> the repoint to an
        // intranet metastore is accepted -> red.
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(hms("thrift://hms.public.example.com:9083"),
                        Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083")));
        Assertions.assertTrue(e.getMessage().contains("SSRF policy"), e.getMessage());
    }

    @Test
    public void alterAcceptsPublicMetastoreAndProbesIt() throws Exception {
        TestCatalog catalog = new TestCatalog(hms("thrift://hms.public.example.com:9083"));

        catalog.validatePropertiesBeforeUpdate(hms("thrift://hms.public.example.com:9083"),
                Collections.singletonMap("hive.metastore.uris", "thrift://hms2.public.example.com:9083"));
        Assertions.assertTrue(checker.probed.contains("hms2.public.example.com:9083"), checker.probed.toString());
    }

    @Test
    public void alterChecksEveryEndpointInAnHaUriList() {
        checker.intranetHost = "192.168.0.7";
        TestCatalog catalog = new TestCatalog(hms("thrift://hms.public.example.com:9083"));

        // Second URI in the HA list resolves to an intranet host and must still be rejected.
        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdate(hms("thrift://hms.public.example.com:9083"),
                        Collections.singletonMap("hive.metastore.uris",
                                "thrift://ok.public.example.com:9083,thrift://192.168.0.7:9083")));
    }

    @Test
    public void alterNotTouchingMetastoreUriSkipsTheGate() throws Exception {
        // A stored uri was gated at its own CREATE/ALTER time; an unrelated ALTER must not re-probe it.
        // MUTATION: drop the touched-keys guard -> the stored intranet uri is probed and rejected -> red.
        checker.intranetHost = "10.0.0.1";
        TestCatalog catalog = new TestCatalog(hms("thrift://10.0.0.1:9083"));

        catalog.validatePropertiesBeforeUpdate(hms("thrift://10.0.0.1:9083"),
                Collections.singletonMap("only_specified_database", "true"));
        Assertions.assertTrue(checker.probed.isEmpty(), "untouched endpoint must not be probed");
    }

    @Test
    public void alterSkipsTheGateWhenNoCheckerInstalled() throws Exception {
        // MUTATION: drop the isEnabled() guard -> the probe runs and the intranet repoint is rejected -> red.
        checker.enabled = false;
        checker.intranetHost = "10.0.0.1";
        TestCatalog catalog = new TestCatalog(hms("thrift://hms.public.example.com:9083"));

        catalog.validatePropertiesBeforeUpdate(hms("thrift://hms.public.example.com:9083"),
                Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083"));
        Assertions.assertTrue(checker.probed.isEmpty(), "disabled checker must not probe");
    }

    // ---- CREATE gate ----

    @Test
    public void createRejectsIntranetMetastore() {
        checker.intranetHost = "127.0.0.1";
        TestCatalog catalog = new TestCatalog(hms("thrift://127.0.0.1:9083"));

        // MUTATION: drop checkEndpointsAgainstSsrf from checkWhenCreating -> CREATE accepts the intranet
        // metastore -> red.
        Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
    }

    private static Map<String, String> hms(String metastoreUris) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.uris", metastoreUris);
        return props;
    }

    /** Records the net-hook bracket and the probed endpoints; throws SecurityException for an intranet host. */
    private static final class RecordingChecker extends SecurityChecker {
        private boolean enabled = true;
        private String intranetHost;
        private final List<String> probed = new ArrayList<>();

        @Override
        public String getSafeJdbcUrl(String originJdbcUrl) {
            return originJdbcUrl;
        }

        @Override
        public void startSSRFChecking(String originUri) {
        }

        @Override
        public void startSSRFChecking() {
        }

        @Override
        public void stopSSRFChecking() {
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        protected void doConnect(String host, int port, int connectTimeoutMs) throws IOException {
            probed.add(host + ":" + port);
            if (host.equals(intranetHost)) {
                throw new SecurityException("simulated SSRFUnsafeConnectionException for " + host);
            }
        }
    }

    /** Keeps the real CREATE/ALTER validation; stubs out what needs a full FE environment. */
    private static final class TestCatalog extends PluginDrivenExternalCatalog {
        TestCatalog(Map<String, String> props) {
            super(1L, "ssrf-gate-catalog", null, props, "", Mockito.mock(Connector.class));
            this.initialized = true;
        }

        @Override
        protected Connector createConnectorFromProperties() {
            return null;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return Mockito.mock(ConnectorSession.class);
        }

        @Override
        ConnectorSecuritySurface securitySurface(Map<String, String> properties) {
            // The type=hms catalog declares its thrift endpoint; the real keys are pinned by
            // HiveConnectorProviderTest.
            return ConnectorSecuritySurface.ofSsrfEndpoints("hive.metastore.uris", "uri");
        }
    }
}
