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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.junit.Before;
import org.junit.Test;

public class PluginMgrTest {

    class TestPlugin extends Plugin implements AuditPlugin {

        @Override
        public boolean eventFilter(short type, short masks) {
            return type == AuditEvent.AUDIT_CONNECTION;
        }

        @Override
        public void exec(AuditEvent event) {
        }
    }


    private Catalog catalog;

    private FakeCatalog fakeCatalog;

    private PluginMgr mgr;

    @Before
    public void setUp() {
        try {
            fakeCatalog = new FakeCatalog();
            catalog = Deencapsulation.newInstance(Catalog.class);

            FakeCatalog.setCatalog(catalog);
            FakeCatalog.setMetaVersion(FeConstants.meta_version);

            FileUtils.deleteQuietly(PluginTestUtil.getTestFile("target"));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target")));
            Files.createDirectory(PluginTestUtil.getTestPath("target"));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target")));

            Config.plugin_dir = PluginTestUtil.getTestPathString("target");

            mgr = new PluginMgr();
            PluginInfo info = new PluginInfo("TestPlugin", PluginInfo.PluginType.AUDIT, "use for test");
            assertTrue(mgr.registerPlugin(info, new TestPlugin()));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInstallPluginZip() {
        try {
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            InstallPluginStmt stmt = new InstallPluginStmt(PluginTestUtil.getTestPathString("auditdemo.zip"));
            mgr.installPlugin(stmt);

            assertEquals(2, mgr.getActivePluginList(PluginInfo.PluginType.AUDIT).size());

            Plugin p = mgr.getActivePlugin("audit_plugin_demo", PluginInfo.PluginType.AUDIT);

            assertNotNull(p);
            assertTrue(p instanceof AuditPlugin);
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_QUERY, AuditEvent.AUDIT_QUERY_START));
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_QUERY, AuditEvent.AUDIT_QUERY_END));
            assertFalse(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_CONNECTION, AuditEvent.AUDIT_QUERY_END));

            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            assertEquals(1, mgr.getAllDynamicPluginInfo().size());
            PluginInfo info = mgr.getAllDynamicPluginInfo().get(0);

            assertEquals("audit_plugin_demo", info.getName());
            assertEquals(PluginInfo.PluginType.AUDIT, info.getType());
            assertEquals("just for test", info.getDescription());
            assertEquals("plugin.AuditPluginDemo", info.getClassName());

            mgr.uninstallPlugin("audit_plugin_demo");

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
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("test_plugin")));

            FileUtils.copyDirectory(PluginTestUtil.getTestPath("test_local_plugin").toFile(),
                    PluginTestUtil.getTestPath("test_plugin").toFile());

            InstallPluginStmt stmt = new InstallPluginStmt(PluginTestUtil.getTestPathString("test_plugin"));
            mgr.installPlugin(stmt);

            assertFalse(Files.exists(PluginTestUtil.getTestPath("test_plugin")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("test_plugin/auditdemo.jar")));

            Plugin p = mgr.getActivePlugin("audit_plugin_demo", PluginInfo.PluginType.AUDIT);

            assertEquals(2, mgr.getActivePluginList(PluginInfo.PluginType.AUDIT).size());

            assertNotNull(p);
            assertTrue(p instanceof AuditPlugin);
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_QUERY, AuditEvent.AUDIT_QUERY_START));
            assertTrue(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_QUERY, AuditEvent.AUDIT_QUERY_END));
            assertFalse(((AuditPlugin) p).eventFilter(AuditEvent.AUDIT_CONNECTION, AuditEvent.AUDIT_QUERY_END));

            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            mgr.uninstallPlugin("audit_plugin_demo");

            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

        } catch (IOException | UserException e) {
            e.printStackTrace();
            assert false;
        }
    }


    @Test
    public void testSerializeBuiltinPlugin() {

        try {
            DataOutputBuffer dob = new DataOutputBuffer();
            DataOutputStream dos = new DataOutputStream(dob);
            mgr.write(dos);

            PluginMgr test = new PluginMgr();

            test.readFields(new DataInputStream(new ByteArrayInputStream(dob.getData())));
            assertEquals(0, test.getAllDynamicPluginInfo().size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSerializeDynamicPlugin() {
        try {
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            InstallPluginStmt stmt = new InstallPluginStmt(PluginTestUtil.getTestPathString("auditdemo.zip"));
            mgr.installPlugin(stmt);

            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));

            assertEquals(1, mgr.getAllDynamicPluginInfo().size());

            DataOutputBuffer dob = new DataOutputBuffer();
            DataOutputStream dos = new DataOutputStream(dob);
            mgr.write(dos);

            PluginMgr test = new PluginMgr();

            assertNotNull(dob);
            assertNotNull(test);
            DataInputStream dis =  new DataInputStream(new ByteArrayInputStream(dob.getData()));
            test.readFields(dis);
            assertEquals(1, test.getAllDynamicPluginInfo().size());

            mgr.uninstallPlugin("audit_plugin_demo");

            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo")));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target/audit_plugin_demo/auditdemo.jar")));
        } catch (IOException | UserException e) {
            e.printStackTrace();
        }
    }


}
