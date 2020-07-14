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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.PluginInfo.PluginType;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class PluginLoaderTest {

    @Before
    public void setUp() {
        try {
            FileUtils.deleteQuietly(PluginTestUtil.getTestFile("target"));
            assertFalse(Files.exists(PluginTestUtil.getTestPath("target")));
            Files.createDirectory(PluginTestUtil.getTestPath("target"));
            assertTrue(Files.exists(PluginTestUtil.getTestPath("target")));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMovePlugin() {
        PluginInfo pf =
                new PluginInfo("test-plugin", PluginType.STORAGE, "test/test", DigitalVersion.CURRENT_DORIS_VERSION,
                        DigitalVersion.JDK_1_8_0, "test/test", "libtest.so", "test/test");

        try {
            PluginLoader util = new DynamicPluginLoader(PluginTestUtil.getTestPathString("source"), pf);
            ((DynamicPluginLoader) util).installPath = PluginTestUtil.getTestPath("target");
            ((DynamicPluginLoader) util).movePlugin();
            assertTrue(Files.isDirectory(PluginTestUtil.getTestPath("source/test-plugin")));
            assertTrue(FileUtils.deleteQuietly(PluginTestUtil.getTestFile("source/test-plugin")));
        } catch (IOException | UserException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDynamicLoadPlugin() {
        try {
            PluginInfo info = new PluginInfo("test", PluginType.STORAGE, "test", DigitalVersion.CURRENT_DORIS_VERSION,
                    DigitalVersion.JDK_1_8_0, "plugin.PluginTest", "libtest.so", "plugin_test.jar");

            DynamicPluginLoader util = new DynamicPluginLoader(PluginTestUtil.getTestPathString(""), info);
            Plugin p = util.dynamicLoadPlugin(true);

            p.init(null, null);
            p.close();
            assertEquals(2, p.flags());

            p.setVariable("test", "value");

            Map<String, String> m = p.variable();

            assertEquals(1, m.size());
            assertTrue(m.containsKey("test"));
            assertEquals("value", m.get("test"));

        } catch (IOException | UserException e) {
            e.printStackTrace();
        }
    }

}
