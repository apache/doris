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

package org.apache.doris.journal.bdbje;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.UnitTestUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BDBJournalCursorTest {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironmentTest.class);
    private static List<String> tmpDirs = new ArrayList<>();

    public static String createTmpDir() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            dorisHome = Files.createTempDirectory("DORIS_HOME").toAbsolutePath().toString();
        }
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        Path mockDir = Paths.get(dorisHome, "fe", "mocked");
        if (!Files.exists(mockDir)) {
            Files.createDirectories(mockDir);
        }
        UUID uuid = UUID.randomUUID();
        File dir = Files.createDirectories(Paths.get(dorisHome, "fe", "mocked", "BDBEnvironmentTest-" + uuid.toString())).toFile();
        if (LOG.isDebugEnabled()) {
            LOG.debug("createTmpDir path {}", dir.getAbsolutePath());
        }
        tmpDirs.add(dir.getAbsolutePath());
        return dir.getAbsolutePath();
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (String dir : tmpDirs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("deleteTmpDir path {}", dir);
            }
            FileUtils.deleteDirectory(new File(dir));
        }
    }

    @RepeatedTest(1)
    public void testNormal() throws Exception {
        Assertions.assertTrue(BDBJournalCursor.getJournalCursor(null, -1, 20) == null);
        Assertions.assertTrue(BDBJournalCursor.getJournalCursor(null, 21, 20) == null);

        int port = UnitTestUtil.findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        if (LOG.isDebugEnabled()) {
            LOG.debug("selfNodeName:{}, selfNodeHostPort:{}", selfNodeName, selfNodeHostPort);
        }

        BDBEnvironment bdbEnvironment = new BDBEnvironment(true, false);
        bdbEnvironment.setup(new File(createTmpDir()), selfNodeName, selfNodeHostPort, selfNodeHostPort);

        Database db = bdbEnvironment.openDatabase("1");
        db.close();

        BDBJournalCursor bdbJournalCursor = BDBJournalCursor.getJournalCursor(bdbEnvironment, 1, 10);
        Assertions.assertTrue(bdbJournalCursor != null);
        Assertions.assertTrue(bdbJournalCursor.next() == null);

        bdbEnvironment.close();

        bdbJournalCursor = BDBJournalCursor.getJournalCursor(bdbEnvironment, 1, 10);
        Assertions.assertTrue(bdbJournalCursor == null);
    }
}
