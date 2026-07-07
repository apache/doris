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

package org.apache.doris.paimon;

import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class PaimonJniScannerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testConstructorAcceptsEmptyProjection() {
        new PaimonJniScanner(128, createBaseParams());
    }

    @Test
    public void testIOManagerOptionHelpers() throws Exception {
        Map<String, String> params = createBaseParams();
        Assert.assertFalse(PaimonJniScanner.isIOManagerEnabled(params));

        params.put(PaimonJniScanner.ENABLE_JNI_IO_MANAGER, "true");
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager");
        params.put(PaimonJniScanner.JNI_IO_MANAGER_TMP_DIR, tempDir.getAbsolutePath());

        Assert.assertTrue(PaimonJniScanner.isIOManagerEnabled(params));
        Assert.assertEquals(tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerTempDirs(params));
        Assert.assertNull(PaimonJniScanner.getIOManagerImplClass(params));
        PaimonJniScanner.createIOManager(tempDir.getAbsolutePath()).close();
        Assert.assertTrue(tempDir.exists());
    }

    @Test
    public void testCreateDefaultAndCustomIOManager() throws Exception {
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager-impl");
        IOManager defaultIOManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        Assert.assertTrue(defaultIOManager instanceof IOManagerImpl);
        defaultIOManager.close();

        Map<String, String> params = createBaseParams();
        params.put(PaimonJniScanner.JNI_IO_MANAGER_IMPL_CLASS, TestIOManager.class.getName());
        Assert.assertEquals(TestIOManager.class.getName(), PaimonJniScanner.getIOManagerImplClass(params));
        IOManager customIOManager = PaimonJniScanner.createIOManager(
                tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerImplClass(params));
        Assert.assertTrue(customIOManager instanceof TestIOManager);
        Assert.assertArrayEquals(new String[] {tempDir.getAbsolutePath()}, customIOManager.tempDirs());
    }

    @Test
    public void testCloseCleansIOManagerTempDirectory() throws Exception {
        File tempDir = temporaryFolder.newFolder("paimon-io-manager-clean");
        IOManager ioManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        FileIOChannel.ID channel = ioManager.createChannel();
        File spillFile = channel.getPathFile();
        Assert.assertTrue(spillFile.createNewFile());
        File spillDir = spillFile.getParentFile();
        Assert.assertTrue(spillDir.exists());

        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        Field ioManagerField = PaimonJniScanner.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(scanner, ioManager);
        Assert.assertEquals("1", scanner.getStatistics().get("counter:PaimonJniIOManagerEnabled"));

        scanner.close();
        Assert.assertFalse(spillDir.exists());
    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "");
        params.put("columns_types", "");
        params.put("paimon_split", "");
        params.put("paimon_predicate", "");
        return params;
    }

    public static class TestIOManager implements IOManager {
        private final String[] tempDirs;

        public TestIOManager(String[] tempDirs) {
            this.tempDirs = tempDirs;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIOChannel.ID createChannel(String channelName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] tempDirs() {
            return tempDirs;
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
