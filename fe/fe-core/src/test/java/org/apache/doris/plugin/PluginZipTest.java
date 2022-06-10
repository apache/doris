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

import org.apache.doris.common.UserException;

import mockit.Expectations;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class PluginZipTest {

    @Before
    public void setUp() {
        try {
            FileUtils.deleteQuietly(PluginTestUtil.getTestFile("target"));
            Assert.assertFalse(Files.exists(PluginTestUtil.getTestPath("target")));
            Files.createDirectory(PluginTestUtil.getTestPath("target"));
            Assert.assertTrue(Files.exists(PluginTestUtil.getTestPath("target")));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDownloadAndValidateZipNormal() {
        PluginZip zip = new PluginZip("source/test.zip", null);
        try {
            // normal
            new Expectations(zip) {
                {
                    zip.getInputStreamFromUrl("source/test.zip");
                    result = PluginTestUtil.openTestFile("source/test.zip");

                    zip.getInputStreamFromUrl("source/test.zip.md5");
                    result = new ByteArrayInputStream(new String("7529db41471ec72e165f96fe9fb92742").getBytes());
                }
            };

            Path zipPath = zip.downloadRemoteZip(PluginTestUtil.getTestPath("target"));
            Assert.assertTrue(Files.exists(zipPath));
            Assert.assertTrue(Files.deleteIfExists(zipPath));

        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testDownloadAndValidateZipNormalWithExpectedMd5sum() {
        PluginZip zip = new PluginZip("source/test.zip", "7529db41471ec72e165f96fe9fb92742");
        try {
            // normal
            new Expectations(zip) {
                {
                    zip.getInputStreamFromUrl("source/test.zip");
                    result = PluginTestUtil.openTestFile("source/test.zip");
                }
            };

            Path zipPath = zip.downloadRemoteZip(PluginTestUtil.getTestPath("target"));
            Assert.assertTrue(Files.exists(zipPath));
            Assert.assertTrue(Files.deleteIfExists(zipPath));

        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testDownloadAndValidateZipMd5Error() {
        PluginZip zip = new PluginZip("source/test.zip", null);
        try {
            new Expectations(zip) {
                {
                    zip.getInputStreamFromUrl("source/test.zip");
                    result = PluginTestUtil.openTestFile("source/test.zip");

                    zip.getInputStreamFromUrl("source/test.zip.md5");
                    result = new ByteArrayInputStream(new String("asdfas").getBytes());
                }
            };

            Path zipPath = zip.downloadRemoteZip(PluginTestUtil.getTestPath("target"));
            Assert.assertFalse(Files.exists(zipPath));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UserException);
            Assert.assertTrue(e.getMessage().contains("MD5 check mismatch"));
        }
    }

    @Test
    public void testDownloadAndValidateZipIOException() {
        PluginZip util = new PluginZip("http://io-exception", null);
        try {
            util.downloadRemoteZip(PluginTestUtil.getTestPath("target"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }


    @Test
    public void testExtract() {
        try {
            Files.copy(PluginTestUtil.getTestPath("source/test.zip"), PluginTestUtil.getTestPath("source/test-a.zip"));

            PluginZip util = new PluginZip(PluginTestUtil.getTestPathString("source/test-a.zip"), null);

            Path actualPath = util.extract(PluginTestUtil.getTestPath("target"));
            Assert.assertTrue(Files.isDirectory(actualPath));

            Path txtPath = FileSystems.getDefault().getPath(actualPath.toString(), "test.txt");
            Assert.assertTrue(Files.exists(txtPath));

            Assert.assertTrue(FileUtils.deleteQuietly(actualPath.toFile()));
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }


    @Test
    public void testDownload() {
        // normal
        try {
            PluginZip util = new PluginZip(PluginTestUtil.getTestPathString("source/test.zip"), null);
            Path p = util.downloadZip(PluginTestUtil.getTestPath("target"));
            Assert.assertTrue(Files.exists(p));

        } catch (IOException | UserException e) {
            e.printStackTrace();
        }

        try {
            PluginZip util = new PluginZip("https://hello:12313/test.zip", null);

            new Expectations(util) {
                {
                    util.downloadRemoteZip((Path) any);
                    result = null;
                }
            };

            Path p = util.downloadZip(PluginTestUtil.getTestPath("target"));
            Assert.assertNull(p);

        } catch (IOException | UserException e) {
            e.printStackTrace();
        }


        // empty sources
        try {
            PluginZip util = new PluginZip("   ", null);

            util.downloadZip(PluginTestUtil.getTestPath("target"));
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
