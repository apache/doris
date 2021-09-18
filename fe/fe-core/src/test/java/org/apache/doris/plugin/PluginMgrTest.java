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

package org.apache.doris.plugin;

import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import avro.shaded.com.google.common.collect.Maps;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PluginMgrTest {

    private static String runningDir = "fe/mocked/PluginMgrTest/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteQuietly(PluginTestUtil.getTestFile("target"));
        assertFalse(Files.exists(PluginTestUtil.getTestPath("target")));
        Files.createDirectory(PluginTestUtil.getTestPath("target"));
        assertTrue(Files.exists(PluginTestUtil.getTestPath("target")));
        Config.plugin_dir = PluginTestUtil.getTestPathString("target");
    }

    @Test
    public void testInstallPluginZip() {
        try {
            // path "target/audit_plugin_demo" is where we are going to install the plugin
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            InstallPluginStmt stmt = new InstallPluginStmt(PluginTestUtil.getTestPathString("auditdemo.zip"), Maps.newHashMap());
            Catalog.getCurrentCatalog().installPlugin(stmt);

            PluginMgr pluginMgr = Catalog.getCurrentPluginMgr();

            assertEquals(2, pluginMgr.getActivePluginList(PluginInfo.PluginType.AUDIT).size());

            Plugin p = pluginMgr.getActivePlugin("audit_plugin_demo", PluginInfo.PluginType.AUDIT);

            assertNotNull(p);
            assertTrue(p instanceof AuditPlugin);
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.EventType.AFTER_QUERY));
            assertFalse(((AuditPlugin) p).eventFilter(AuditEvent.EventType.BEFORE_QUERY));

            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            assertEquals(1, pluginMgr.getAllDynamicPluginInfo().size());
            PluginInfo info = pluginMgr.getAllDynamicPluginInfo().get(0);

            assertEquals("audit_plugin_demo", info.getName());
            assertEquals(PluginInfo.PluginType.AUDIT, info.getType());
            assertEquals("just for test", info.getDescription());
            assertEquals("plugin.AuditPluginDemo", info.getClassName());

            pluginMgr.uninstallPlugin("audit_plugin_demo");

            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

        } catch (IOException | UserException e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testInstallPluginLocal() {
        try {
            // path "target/audit_plugin_demo" is where we are going to install the plugin
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            InstallPluginStmt stmt = new InstallPluginStmt(PluginTestUtil.getTestPathString("test_local_plugin"), Maps.newHashMap());
            Catalog.getCurrentCatalog().installPlugin(stmt);

            PluginMgr pluginMgr = Catalog.getCurrentPluginMgr();

            assertTrue(Files.exists(PluginTestUtil.getTestPath("test_local_plugin")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("test_local_plugin/auditdemo.jar")));

            Plugin p = pluginMgr.getActivePlugin("audit_plugin_demo", PluginInfo.PluginType.AUDIT);

            assertEquals(2, pluginMgr.getActivePluginList(PluginInfo.PluginType.AUDIT).size());

            assertNotNull(p);
            assertTrue(p instanceof AuditPlugin);
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.EventType.AFTER_QUERY));
            assertFalse(((AuditPlugin) p).eventFilter(AuditEvent.EventType.BEFORE_QUERY));

            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            testSerializeBuiltinPlugin(pluginMgr);
            pluginMgr.uninstallPlugin("audit_plugin_demo");

            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

        } catch (IOException | UserException e) {
            e.printStackTrace();
            assert false;
        }
    }

    private void testSerializeBuiltinPlugin(PluginMgr mgr) {
        try {
            DataOutputBuffer dob = new DataOutputBuffer();
            DataOutputStream dos = new DataOutputStream(dob);
            mgr.write(dos);

            PluginMgr test = new PluginMgr();

            test.readFields(new DataInputStream(new ByteArrayInputStream(dob.getData())));
            assertEquals(1, test.getAllDynamicPluginInfo().size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
