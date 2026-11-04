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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;

import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * CREATE / ALTER CATALOG must apply the operator's SSRF policy to every metastore endpoint an HMS catalog
 * would make the FE connect to, rejecting an intranet target. The judgment is delegated to the third-party
 * {@code SecurityChecker} SDK (absent in CI, so a recording fake stands in); the gate runs on CREATE
 * ({@code checkWhenCreating}) and on ALTER ({@code validatePropertiesBeforeUpdateUnlocked}, which CatalogMgr runs
 * before it takes its write lock) but never on replay, and it is a no-op when no checker is installed.
 */
public class HMSExternalCatalogSsrfGateTest {

    private RecordingChecker checker;

    @BeforeEach
    public void setUp() {
        checker = new RecordingChecker();
        SecurityChecker.setInstanceForTest(checker);
    }

    @AfterEach
    public void tearDown() {
        // Revert to the Config-driven singleton so a reused surefire fork is not left with the fake.
        SecurityChecker.setInstanceForTest(null);
    }

    // ---- static helper: endpoint parsing ----

    @Test
    public void parseEndpointExtractsHostAndPort() {
        HostAndPort schemed = HMSExternalCatalog.parseEndpoint("thrift://host1:9083");
        Assertions.assertNotNull(schemed);
        Assertions.assertEquals("host1", schemed.getHost());
        Assertions.assertEquals(9083, schemed.getPort());

        HostAndPort bare = HMSExternalCatalog.parseEndpoint("host2:9083");
        Assertions.assertNotNull(bare);
        Assertions.assertEquals("host2", bare.getHost());
        Assertions.assertEquals(9083, bare.getPort());
    }

    @Test
    public void parseEndpointReturnsNullWhenNoHostPort() {
        Assertions.assertNull(HMSExternalCatalog.parseEndpoint(""));
        Assertions.assertNull(HMSExternalCatalog.parseEndpoint("thrift://host-without-port"));
        Assertions.assertNull(HMSExternalCatalog.parseEndpoint("thrift://"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"thrift://host1:9083?ignored", "thrift://host1:9083#ignored",
            "thrift://user@host1:9083", "thrift://user:password@host1:9083/path?ignored#ignored",
            "thrift://host1:9083/path", "host1:9083/path"})
    public void parseEndpointUsesUriDestination(String endpoint) {
        HostAndPort parsed = HMSExternalCatalog.parseEndpoint(endpoint);
        Assertions.assertNotNull(parsed);
        Assertions.assertEquals("host1", parsed.getHost());
        Assertions.assertEquals(9083, parsed.getPort());
    }

    @ParameterizedTest
    @ValueSource(strings = {"thrift://[::1]:9083", "thrift://user@[::1]:9083?ignored#ignored", "[::1]:9083"})
    public void parseEndpointPreservesIpv6Destination(String endpoint) {
        HostAndPort parsed = HMSExternalCatalog.parseEndpoint(endpoint);
        Assertions.assertNotNull(parsed);
        Assertions.assertEquals("::1", parsed.getHost());
        Assertions.assertEquals(9083, parsed.getPort());
    }

    // ---- ALTER gate ----

    @Test
    public void alterRejectsRepointingMetastoreToIntranet() {
        checker.intranetHost = "10.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        // MUTATION: drop checkEndpointsAgainstSsrf from validatePropertiesBeforeUpdateUnlocked -> the repoint to
        // an intranet metastore is accepted -> red.
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(hms("thrift://hms.public.example.com:9083"),
                        Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083")));
        Assertions.assertTrue(e.getMessage().contains("SSRF policy"), e.getMessage());
    }

    @Test
    public void alterAcceptsPublicMetastoreAndProbesIt() throws Exception {
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        catalog.validatePropertiesBeforeUpdateUnlocked(hms("thrift://hms.public.example.com:9083"),
                Collections.singletonMap("hive.metastore.uris", "thrift://hms2.public.example.com:9083"));
        Assertions.assertTrue(checker.probed.contains("hms2.public.example.com:9083"), checker.probed.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"thrift://10.0.0.1:9083?ignored", "thrift://10.0.0.1:9083#ignored",
            "thrift://user@10.0.0.1:9083", "thrift://user:password@10.0.0.1:9083/path?ignored#ignored"})
    public void alterRejectsUriDestinationWithSuffixes(String endpoint) {
        checker.intranetHost = "10.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(),
                        Collections.singletonMap("hive.metastore.uris", endpoint)));
        Assertions.assertEquals(Collections.singletonList("10.0.0.1:9083"), checker.probed);
    }

    @Test
    public void alterChecksEveryEndpointInAnHaUriList() {
        checker.intranetHost = "192.168.0.7";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        // Second URI in the HA list resolves to an intranet host and must still be rejected.
        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(hms("thrift://hms.public.example.com:9083"),
                        Collections.singletonMap("hive.metastore.uris",
                                "thrift://ok.public.example.com:9083,thrift://192.168.0.7:9083?ignored#ignored")));
    }

    @Test
    public void alterTouchingOnlyTheUriAliasIsProbed() {
        // uri is the documented alias HMSBaseProperties binds to hive.metastore.uris.
        checker.intranetHost = "127.0.0.1";
        Map<String, String> stored = new HashMap<>();
        stored.put("type", "hms");
        stored.put("uri", "thrift://hms.public.example.com:9083");
        HMSExternalCatalog catalog = catalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(),
                        Collections.singletonMap("uri", "thrift://127.0.0.1:9083")));
        Assertions.assertEquals(Collections.singletonList("127.0.0.1:9083"), checker.probed);
    }

    @Test
    public void alterTouchingOnlyModernMetastoreKeyIsProbed() {
        // The client's MetastoreConf reads metastore.thrift.uris before hive.metastore.uris. The key is declared
        // even though HMSBaseProperties does not hand it to the client today, so this ALTER alone is probed.
        checker.intranetHost = "127.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(),
                        Collections.singletonMap("metastore.thrift.uris", "thrift://127.0.0.1:9083")));
        // A touched endpoint key probes every declared endpoint of the candidate, the stored legacy uri included.
        Assertions.assertTrue(checker.probed.contains("127.0.0.1:9083"), checker.probed.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"hive.metastore.uris", "uri", "metastore.thrift.uris"})
    public void alterRejectsPlaceholderEndpointWithoutProbing(String key) {
        // MUTATION: drop the placeholder check -> URI parsing fails, the endpoint is skipped and the ALTER is
        // accepted, although the client's HiveConf would expand hive.metastore.uris to 127.0.0.1 -> red.
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));
        Map<String, String> update = new HashMap<>();
        update.put(key, "thrift://${hive.ssrf.target}:9083");
        // hive.-prefixed, so HMSBaseProperties copies it into the client's HiveConf, where it is expanded.
        update.put("hive.ssrf.target", "127.0.0.1");

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(), update));
        Assertions.assertTrue(e.getMessage().contains("placeholder"), e.getMessage());
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    @Test
    public void alterOfThePropertyAStoredPlaceholderRefersToIsRejected() {
        // A catalog stored before a checker was configured can carry a placeholder; changing only the property
        // it refers to repoints the client without touching an endpoint key.
        // MUTATION: move the placeholder check after the touched guard -> the ALTER is accepted -> red.
        Map<String, String> stored = hms("thrift://${hive.ssrf.target}:9083");
        stored.put("hive.ssrf.target", "hms.public.example.com");
        HMSExternalCatalog catalog = catalog(stored);

        Assertions.assertThrows(DdlException.class,
                () -> catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(),
                        Collections.singletonMap("hive.ssrf.target", "127.0.0.1")));
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    @Test
    public void alterAcceptsPlaceholderWhenNoCheckerInstalled() throws Exception {
        // MUTATION: run the placeholder check before the isEnabled() guard -> rejected -> red.
        checker.enabled = false;
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        catalog.validatePropertiesBeforeUpdateUnlocked(catalog.getProperties(),
                Collections.singletonMap("hive.metastore.uris", "thrift://${hive.ssrf.target}:9083"));
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    @Test
    public void alterNotTouchingMetastoreUriSkipsTheGate() throws Exception {
        // A stored uri was gated at its own CREATE/ALTER time; an unrelated ALTER must not re-probe it.
        // MUTATION: drop the touched-keys guard -> the stored intranet uri is probed and rejected -> red.
        checker.intranetHost = "10.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://10.0.0.1:9083"));

        catalog.validatePropertiesBeforeUpdateUnlocked(hms("thrift://10.0.0.1:9083"),
                Collections.singletonMap("only_specified_database", "true"));
        Assertions.assertTrue(checker.probed.isEmpty(), "untouched endpoint must not be probed");
    }

    @Test
    public void alterSkipsTheGateWhenNoCheckerInstalled() throws Exception {
        // MUTATION: drop the isEnabled() guard -> the probe runs and the intranet repoint is rejected -> red.
        checker.enabled = false;
        checker.intranetHost = "10.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        catalog.validatePropertiesBeforeUpdateUnlocked(hms("thrift://hms.public.example.com:9083"),
                Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083"));
        Assertions.assertTrue(checker.probed.isEmpty(), "disabled checker must not probe");
    }

    @Test
    public void lockedAlterValidationNeverProbes() throws Exception {
        // validatePropertiesBeforeUpdate runs under CatalogMgr's write lock, so it must not reach the network.
        // MUTATION: call checkEndpointsAgainstSsrf from validatePropertiesBeforeUpdate -> probed -> red.
        checker.intranetHost = "10.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://hms.public.example.com:9083"));

        Assertions.assertTrue(catalog.validatePropertiesBeforeUpdate(catalog.getProperties(),
                Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083")));
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    // ---- CREATE gate ----

    @Test
    public void createRejectsIntranetMetastore() {
        checker.intranetHost = "127.0.0.1";
        HMSExternalCatalog catalog = catalog(hms("thrift://127.0.0.1:9083"));

        // MUTATION: drop checkEndpointsAgainstSsrf from checkWhenCreating -> CREATE accepts the intranet
        // metastore -> red.
        Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
    }

    @Test
    public void createAcceptsPublicEndpointsAndProbesEveryDeclaredKey() throws Exception {
        Map<String, String> properties = hms("thrift://hms1.public.example.com:9083");
        properties.put("uri", "thrift://hms2.public.example.com:9083");
        properties.put("metastore.thrift.uris", "thrift://hms3.public.example.com:9083");
        properties.put("test_connection", "false");
        HMSExternalCatalog catalog = catalog(properties);

        catalog.checkWhenCreating();
        Assertions.assertEquals(Arrays.asList("hms1.public.example.com:9083", "hms2.public.example.com:9083",
                "hms3.public.example.com:9083"), checker.probed);
    }

    @ParameterizedTest
    @ValueSource(strings = {"thrift://127.0.0.1:9083?ignored", "thrift://127.0.0.1:9083#ignored",
            "thrift://user@127.0.0.1:9083", "thrift://user:password@127.0.0.1:9083/path?ignored#ignored"})
    public void createRejectsUriDestinationWithConnectionTestDisabled(String endpoint) {
        checker.intranetHost = "127.0.0.1";
        Map<String, String> properties = hms(endpoint);
        properties.put("test_connection", "false");
        HMSExternalCatalog catalog = catalog(properties);

        Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
        Assertions.assertEquals(Collections.singletonList("127.0.0.1:9083"), checker.probed);
    }

    @Test
    public void createProbesModernMetastoreKeyBesideLegacyKey() {
        // MetastoreConf prefers metastore.thrift.uris when both keys reach the client, so a public legacy uri must
        // not be the only value probed.
        checker.intranetHost = "127.0.0.1";
        Map<String, String> properties = hms("thrift://hms.public.example.com:9083");
        properties.put("metastore.thrift.uris", "thrift://127.0.0.1:9083");
        properties.put("test_connection", "false");
        HMSExternalCatalog catalog = catalog(properties);

        Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
        Assertions.assertTrue(checker.probed.contains("127.0.0.1:9083"), checker.probed.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"thrift://${hive.ssrf.target}:9083", "${hive.ssrf.target}:9083", "${hive.ssrf.target}",
            "thrift://${env.HOSTNAME}:9083"})
    public void createRejectsPlaceholderEndpointWithConnectionTestDisabled(String endpoint) {
        // Without the placeholder check the schemed forms fail URI parsing and are skipped, and
        // "${hive.ssrf.target}:9083" probes a host literally named "${hive.ssrf.target}", whose DNS failure is
        // ignored.
        Map<String, String> properties = hms(endpoint);
        properties.put("hive.ssrf.target", "127.0.0.1");
        properties.put("test_connection", "false");
        HMSExternalCatalog catalog = catalog(properties);

        Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"hive.metastore.uris", "uri", "metastore.thrift.uris"})
    public void createRejectsPlaceholderUnderEveryDeclaredKey(String key) {
        // On CREATE the gate runs before the property validation, so every declared key reaches the
        // placeholder rule, the uri alias included.
        Map<String, String> properties = hms("thrift://hms.public.example.com:9083");
        properties.put(key, "thrift://${hive.ssrf.target}:9083");
        properties.put("hive.ssrf.target", "127.0.0.1");
        properties.put("test_connection", "false");
        HMSExternalCatalog catalog = catalog(properties);

        DdlException e = Assertions.assertThrows(DdlException.class, catalog::checkWhenCreating);
        Assertions.assertTrue(e.getMessage().contains("placeholder")
                && e.getMessage().contains("Property '" + key + "'"), e.getMessage());
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    // ---- wiring through CatalogFactory: CREATE runs the gate, replay never does ----

    @Test
    public void createThroughCatalogFactoryRunsTheGate() {
        checker.intranetHost = "127.0.0.1";
        CreateCatalogCommand cmd = new CreateCatalogCommand("ssrf_gate_catalog", false, null, "",
                hms("thrift://127.0.0.1:9083"));

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> CatalogFactory.createFromCommand(1L, cmd));
        Assertions.assertTrue(e.getMessage().contains("SSRF policy"), e.getMessage());
    }

    @Test
    public void replayThroughCatalogFactoryNeverProbes() throws Exception {
        // A catalog in the edit log was gated when it was created; replay must not reach the network.
        checker.intranetHost = "127.0.0.1";
        CatalogLog log = new CatalogLog();
        log.setCatalogId(1L);
        log.setCatalogName("ssrf_gate_catalog");
        log.setComment("");
        log.setProps(hms("thrift://127.0.0.1:9083"));

        CatalogIf catalog = CatalogFactory.createFromLog(log);
        Assertions.assertTrue(catalog instanceof HMSExternalCatalog, String.valueOf(catalog));
        Assertions.assertTrue(checker.probed.isEmpty(), checker.probed.toString());
    }

    // ---- ALTER through CatalogMgr: the probe runs before the write lock is taken ----

    @Test
    public void alterThroughCatalogMgrProbesOutsideTheWriteLock() throws Exception {
        // MUTATION: run the gate from validatePropertiesBeforeUpdate, under CatalogMgr's write lock -> the probe
        // sees the lock held -> red.
        CatalogMgr manager = new CatalogMgr();
        AlterableHmsCatalog catalog = new AlterableHmsCatalog(hms("thrift://hms.public.example.com:9083"));
        addCatalog(manager, catalog);
        checker.catalogMgrLock = catalogMgrLock(manager);
        List<Map<String, String>> journaled = new ArrayList<>();

        try (MockedStatic<Env> mockedEnv = mockEnv(journaled)) {
            manager.alterCatalogProps(catalog.getName(),
                    Collections.singletonMap("hive.metastore.uris", "thrift://hms2.public.example.com:9083"));
        }

        Assertions.assertEquals(Collections.singletonList("hms2.public.example.com:9083"), checker.probed);
        Assertions.assertEquals(Collections.singletonList(false), checker.catalogMgrLockHeld);
        Assertions.assertEquals("thrift://hms2.public.example.com:9083",
                catalog.getProperties().get("hive.metastore.uris"));
        Assertions.assertEquals(1, journaled.size(), journaled.toString());
    }

    @Test
    public void alterThroughCatalogMgrRejectsIntranetWithoutChangingTheCatalog() throws Exception {
        checker.intranetHost = "10.0.0.1";
        CatalogMgr manager = new CatalogMgr();
        AlterableHmsCatalog catalog = new AlterableHmsCatalog(hms("thrift://hms.public.example.com:9083"));
        addCatalog(manager, catalog);
        checker.catalogMgrLock = catalogMgrLock(manager);
        List<Map<String, String>> journaled = new ArrayList<>();

        try (MockedStatic<Env> mockedEnv = mockEnv(journaled)) {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> manager.alterCatalogProps(catalog.getName(),
                            Collections.singletonMap("hive.metastore.uris", "thrift://10.0.0.1:9083")));
            Assertions.assertTrue(e.getMessage().contains("SSRF policy"), e.getMessage());
        }

        Assertions.assertEquals(Collections.singletonList(false), checker.catalogMgrLockHeld);
        Assertions.assertEquals("thrift://hms.public.example.com:9083",
                catalog.getProperties().get("hive.metastore.uris"));
        Assertions.assertTrue(journaled.isEmpty(), journaled.toString());
    }

    @Test
    public void alterThroughCatalogMgrChecksAgainWhenTheCatalogChangesDuringTheProbe() throws Exception {
        // Another ALTER commits while this one probes outside the lock, so the probed snapshot is stale: the ALTER
        // must check the current properties again before it applies.
        // MUTATION: drop the snapshot comparison under the write lock -> applied after a single probe -> red.
        CatalogMgr manager = new CatalogMgr();
        AlterableHmsCatalog catalog = new AlterableHmsCatalog(hms("thrift://hms.public.example.com:9083"));
        addCatalog(manager, catalog);
        checker.catalogMgrLock = catalogMgrLock(manager);
        checker.duringFirstProbe = () -> {
            try {
                manager.alterCatalogProps(catalog.getName(),
                        Collections.singletonMap("only_specified_database", "true"));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
        List<Map<String, String>> journaled = new ArrayList<>();

        try (MockedStatic<Env> mockedEnv = mockEnv(journaled)) {
            manager.alterCatalogProps(catalog.getName(),
                    Collections.singletonMap("hive.metastore.uris", "thrift://hms2.public.example.com:9083"));
        }

        Assertions.assertEquals(Arrays.asList("hms2.public.example.com:9083", "hms2.public.example.com:9083"),
                checker.probed);
        Assertions.assertEquals(Arrays.asList(false, false), checker.catalogMgrLockHeld);
        Map<String, String> properties = catalog.getProperties();
        Assertions.assertEquals("true", properties.get("only_specified_database"));
        Assertions.assertEquals("thrift://hms2.public.example.com:9083", properties.get("hive.metastore.uris"));
        Assertions.assertEquals(2, journaled.size(), journaled.toString());
    }

    @Test
    public void alterThroughCatalogMgrFailsCleanlyWhenTheCatalogIsDroppedDuringTheProbe() throws Exception {
        // A DROP CATALOG that commits while the ALTER probes outside the lock leaves the probed object detached.
        // MUTATION: drop the catalog identity check under the write lock -> the ALTER is applied to the dropped
        // object and fails with a NullPointerException instead of "No catalog found" -> red.
        CatalogMgr manager = new CatalogMgr();
        AlterableHmsCatalog catalog = new AlterableHmsCatalog(hms("thrift://hms.public.example.com:9083"));
        addCatalog(manager, catalog);
        checker.duringFirstProbe = () -> removeCatalog(manager, catalog);
        List<Map<String, String>> journaled = new ArrayList<>();

        try (MockedStatic<Env> mockedEnv = mockEnv(journaled)) {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> manager.alterCatalogProps(catalog.getName(),
                            Collections.singletonMap("hive.metastore.uris", "thrift://hms2.public.example.com:9083")));
            Assertions.assertTrue(e.getMessage().contains("No catalog found"), e.getMessage());
        }
        Assertions.assertTrue(journaled.isEmpty(), journaled.toString());
    }

    private static HMSExternalCatalog catalog(Map<String, String> props) {
        return new HMSExternalCatalog(1L, "ssrf-gate-catalog", null, props, "");
    }

    private static Map<String, String> hms(String metastoreUris) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.uris", metastoreUris);
        return props;
    }

    private static void addCatalog(CatalogMgr manager, HMSExternalCatalog catalog) throws Exception {
        Field ids = CatalogMgr.class.getDeclaredField("idToCatalog");
        ids.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, CatalogIf> idToCatalog = (Map<Long, CatalogIf>) ids.get(manager);
        idToCatalog.put(catalog.getId(), catalog);
        Field names = CatalogMgr.class.getDeclaredField("nameToCatalog");
        names.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, CatalogIf> nameToCatalog = (Map<String, CatalogIf>) names.get(manager);
        nameToCatalog.put(catalog.getName(), catalog);
    }

    /** Unregisters the catalog the way a committed DROP CATALOG leaves CatalogMgr's maps. */
    private static void removeCatalog(CatalogMgr manager, HMSExternalCatalog catalog) {
        try {
            Field ids = CatalogMgr.class.getDeclaredField("idToCatalog");
            ids.setAccessible(true);
            ((Map<?, ?>) ids.get(manager)).remove(catalog.getId());
            Field names = CatalogMgr.class.getDeclaredField("nameToCatalog");
            names.setAccessible(true);
            ((Map<?, ?>) names.get(manager)).remove(catalog.getName());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ReentrantReadWriteLock catalogMgrLock(CatalogMgr manager) throws Exception {
        Field lock = CatalogMgr.class.getDeclaredField("lock");
        lock.setAccessible(true);
        return (ReentrantReadWriteLock) lock.get(manager);
    }

    /** An Env whose edit log records each journaled ALTER and whose cache manager runs fenced actions inline. */
    private static MockedStatic<Env> mockEnv(List<Map<String, String>> journaled) {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(Mockito.anyLong(), Mockito.any()))
                .thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(1)).get());
        Mockito.doAnswer(invocation -> {
            CatalogLog log = invocation.getArgument(1);
            journaled.add(new HashMap<>(log.getNewProps()));
            return null;
        }).when(editLog).logCatalogLog(Mockito.eq(OperationType.OP_ALTER_CATALOG_PROPS), Mockito.any());
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }

    /** Records the net-hook bracket and the probed endpoints; throws SecurityException for an intranet host. */
    private static final class RecordingChecker extends SecurityChecker {
        private boolean enabled = true;
        private String intranetHost;
        private final List<String> probed = new ArrayList<>();
        // Set by the CatalogMgr tests: whether this thread held CatalogMgr's lock at each probe.
        private ReentrantReadWriteLock catalogMgrLock;
        private final List<Boolean> catalogMgrLockHeld = new ArrayList<>();
        // Runs once, inside the first probe, to simulate a DDL that commits while the probe is in flight.
        private Runnable duringFirstProbe;

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
            if (catalogMgrLock != null) {
                catalogMgrLockHeld.add(catalogMgrLock.isWriteLockedByCurrentThread()
                        || catalogMgrLock.getReadHoldCount() > 0);
            }
            if (duringFirstProbe != null) {
                Runnable action = duringFirstProbe;
                duringFirstProbe = null;
                action.run();
            }
            if (host.equals(intranetHost)) {
                throw new SecurityException("simulated SSRFUnsafeConnectionException for " + host);
            }
        }
    }

    /** An HMS catalog whose committed ALTER skips the cache and client reset, which needs a running FE. */
    private static final class AlterableHmsCatalog extends HMSExternalCatalog {
        AlterableHmsCatalog(Map<String, String> props) {
            super(2L, "ssrf_gate_alter_catalog", null, props, "");
        }

        @Override
        public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        }
    }
}
