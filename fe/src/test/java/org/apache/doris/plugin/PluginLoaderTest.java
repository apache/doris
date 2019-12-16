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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Version;
import org.junit.Before;
import org.junit.Test;

import mockit.Expectations;

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
    public void testPluginRead() {
        try {
            PluginInfo info = PluginInfo.readFromProperties(PluginTestUtil.getTestPath("source"),
                    "test");

            assertEquals("plugin_test", info.getName());
            assertEquals(PluginType.STORAGE, info.getType());
            assertTrue(Version.CURRENT_DORIS_VERSION.onOrAfter(info.getVersion()));
            assertTrue(Version.JDK_9_0_0.onOrAfter(info.getJavaVersion()));
            assertTrue(Version.JDK_1_8_0.before(info.getJavaVersion()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDownloadAndValidateZipNormal() {
        PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("target"));
        try {
            // normal
            new Expectations(util) {
                {
                    util.getRemoteInputStream("test/test");
                    result = PluginTestUtil.openTestFile("source/test.test.zip");

                    util.getRemoteInputStream("test.test.md5");
                    result = new ByteArrayInputStream(new String("7529db41471ec72e165f96fe9fb92742").getBytes());
                }
            };

            Path zipPath = util.downloadAndValidateZip("test/test");
            assertTrue(Files.exists(zipPath));
            assertTrue(Files.deleteIfExists(zipPath));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDownloadAndValidateZipMd5Error() {
        PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("target"));
        try {
            new Expectations(util) {
                {
                    util.getRemoteInputStream("test/test");
                    result = PluginTestUtil.openTestFile("source/test.test.zip");

                    util.getRemoteInputStream("test.test.md5");
                    result = new ByteArrayInputStream(new String("asdfas").getBytes());
                }
            };

            Path zipPath = util.downloadAndValidateZip("test/test");
            assertFalse(Files.exists(zipPath));
        } catch (Exception e) {
            assertTrue(e instanceof UserException);
            assertTrue(e.getMessage().contains("MD5 check mismatch"));
        }
    }

    @Test
    public void testDownloadAndValidateZipIOException() {
        PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("target"));
        try {
            Path zipPath = util.downloadAndValidateZip("http://io-exception");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
        }
    }

    @Test
    public void testDownload() {
        PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("target"));
        try {
            new Expectations(util) {
                {
                    util.downloadAndValidateZip(anyString);
                    result = null;
                }

            };

            Path p = util.download(PluginTestUtil.getTestPathString("source/test.test.zip"));
            assertTrue(Files.exists(p));

            p = util.download("https://hello:12313/test.zip");
            assertNull(p);
        } catch (IOException | UserException e) {
            e.printStackTrace();
        }

        try {
            util.download("   ");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testUnzip() {
        try {
            new Expectations(Files.class) {
                {
                    Files.newInputStream((Path) any);
                    result = PluginTestUtil.openTestFile("source/test.test.zip");
                }
            };

            PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("target"));

            Path actualPath = util.unzip(null);
            assertTrue(Files.isDirectory(actualPath));

            Path txtPath = FileSystems.getDefault().getPath(actualPath.toString(), "test.test.txt");
            assertTrue(Files.exists(txtPath));

            assertTrue(FileUtils.deleteQuietly(actualPath.toFile()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMovePlugin() {
        PluginInfo pf =
                new PluginInfo("test.test-plugin", PluginType.STORAGE, "test/test", Version.CURRENT_DORIS_VERSION,
                        Version.JDK_1_8_0, "test/test", "libtest.so", "test/test");

        pf.setInstallPath(PluginTestUtil.getTestPathString("target"));

        try {
            PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString("source"));
            util.movePlugin(pf);
            assertTrue(Files.isDirectory(PluginTestUtil.getTestPath("source/test.test-plugin")));
            assertTrue(FileUtils.deleteQuietly(PluginTestUtil.getTestFile("source/test.test-plugin")));
        } catch (IOException | UserException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDynamicLoadPlugin() {
        try {

            PluginLoader util = new PluginLoader(PluginTestUtil.getTestPathString(""));
            System.out.println(PluginTestUtil.getTestPath(""));

            PluginInfo info = new PluginInfo("test", PluginType.STORAGE, "test", Version.CURRENT_DORIS_VERSION,
                    Version.JDK_1_8_0, "plugin.PluginTest", "libtest.so", "plugin_test.jar");
            Plugin p = util.dynamicLoadPlugin(info, PluginTestUtil.getTestPath(""));

            p.init();
            p.close();
            p.flags();

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
