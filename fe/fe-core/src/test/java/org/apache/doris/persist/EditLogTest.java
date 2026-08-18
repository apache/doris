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

package org.apache.doris.persist;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.journal.bdbje.Timestamp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class EditLogTest {
    private String meta = "editLogTestDir/";
    private String originalEditLogType;
    private int originalEditLogRollNum;
    private int originalCloudEditLogRollIntervalSecond;
    private String originalDeployMode;
    private String originalCloudUniqueId;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUpEditLogRollConfig() {
        originalEditLogType = Config.edit_log_type;
        originalEditLogRollNum = Config.edit_log_roll_num;
        originalCloudEditLogRollIntervalSecond = Config.cloud_edit_log_roll_interval_second;
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;

        Config.edit_log_type = "local";
        Config.edit_log_roll_num = Integer.MAX_VALUE;
        Config.cloud_edit_log_roll_interval_second = 3600;
        Config.cloud_unique_id = "";
    }

    @After
    public void restoreEditLogRollConfig() {
        Config.edit_log_type = originalEditLogType;
        Config.edit_log_roll_num = originalEditLogRollNum;
        Config.cloud_edit_log_roll_interval_second = originalCloudEditLogRollIntervalSecond;
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    public void mkdir() {
        File dir = new File(meta);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit) {
        File imageFile = new File(meta + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(meta + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(meta + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(meta + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir() {
        File dir = new File(meta);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testWriteLog() throws IOException {

    }

    @Test
    public void test() {

    }

    @Test
    public void testCloudModeTimeBasedEditLogRoll() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = temporaryFolder.newFolder("time_based_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.2").exists());
                long txId = Deencapsulation.getField(editLog, "txId");
                Assert.assertEquals(0L, txId);
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testNonCloudModeDoesNotRollEditLogByTime() throws Exception {
        Config.deploy_mode = "share_nothing";

        File imageDir = temporaryFolder.newFolder("non_cloud_time_based_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.logTimestamp(new Timestamp());

                Assert.assertFalse(new File(imageDir, "edits.2").exists());

                Config.edit_log_roll_num = 2;
                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.3").exists());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testRollEditLogResetsCloudRollTime() throws Exception {
        Config.deploy_mode = "cloud";

        File imageDir = temporaryFolder.newFolder("reset_time_after_roll");
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            EditLog editLog = new EditLog("test");
            editLog.open();
            try {
                editLog.logTimestamp(new Timestamp());
                Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                editLog.rollEditLog();
                editLog.logTimestamp(new Timestamp());

                Assert.assertTrue(new File(imageDir, "edits.2").exists());
                Assert.assertFalse(new File(imageDir, "edits.3").exists());
            } finally {
                editLog.close();
            }
        }
    }

    @Test
    public void testNonPositiveCloudEditLogRollIntervalDisablesTimeBasedRoll() throws Exception {
        Config.deploy_mode = "cloud";
        int[] disabledIntervals = {0, -1};
        for (int i = 0; i < disabledIntervals.length; i++) {
            Config.cloud_edit_log_roll_interval_second = disabledIntervals[i];
            File imageDir = temporaryFolder.newFolder("disabled_time_based_roll_" + i);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getImageDir()).thenReturn(imageDir.getAbsolutePath());
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                EditLog editLog = new EditLog("test");
                editLog.open();
                try {
                    Deencapsulation.setField(editLog, "lastEditLogRollTimeMs",
                            System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

                    editLog.logTimestamp(new Timestamp());

                    Assert.assertFalse(new File(imageDir, "edits.2").exists());

                    Config.edit_log_roll_num = 2;
                    editLog.logTimestamp(new Timestamp());

                    Assert.assertTrue(new File(imageDir, "edits.3").exists());
                    Config.edit_log_roll_num = Integer.MAX_VALUE;
                } finally {
                    editLog.close();
                }
            }
        }
    }
}
