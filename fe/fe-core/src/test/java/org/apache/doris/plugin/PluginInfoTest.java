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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.junit.Before;
import org.junit.Test;

public class PluginInfoTest {
    private Catalog catalog;

    private FakeCatalog fakeCatalog;

    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();
        catalog = Deencapsulation.newInstance(Catalog.class);

        FakeCatalog.setCatalog(catalog);
        FakeCatalog.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void testPluginRead() {
        try {
            PluginInfo info = PluginInfo.readFromProperties(PluginTestUtil.getTestPath("source"),
                    "test");

            assertEquals("plugin_test", info.getName());
            assertEquals(PluginType.STORAGE, info.getType());
            assertTrue(DigitalVersion.CURRENT_DORIS_VERSION.onOrAfter(info.getVersion()));
            assertTrue(DigitalVersion.JDK_9_0_0.onOrAfter(info.getJavaVersion()));
            assertTrue(DigitalVersion.JDK_1_8_0.before(info.getJavaVersion()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPluginSerialized() throws IOException {
        PluginInfo info = new PluginInfo();
        info.name = "plugin-name";
        info.type = PluginType.AUDIT;
        info.description = "plugin description";
        info.version = DigitalVersion.CURRENT_DORIS_VERSION;
        info.javaVersion = DigitalVersion.JDK_1_8_0;
        info.className = "hello.jar";
        info.soName = "hello.so";
        info.source = "test";
        info.properties.put("md5sum", "cf0c536b8f2a0a0690b44d783d019e90");

        DataOutputBuffer dob = new DataOutputBuffer();
        DataOutputStream dos = new DataOutputStream(dob);
        info.write(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dob.getData()));
        PluginInfo pi = PluginInfo.read(dis);
        assertFalse(pi.properties.isEmpty());
        assertEquals("cf0c536b8f2a0a0690b44d783d019e90", pi.properties.get("md5sum"));
        assertEquals(info, pi);
    }
}
