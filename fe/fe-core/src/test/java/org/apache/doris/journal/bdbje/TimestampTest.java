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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TimestampTest {
    private static final Logger LOG = LogManager.getLogger(TimestampTest.class);
    private static List<String> testFiles = new ArrayList<>();

    public static String createTestFile() throws Exception {
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
        File testFile = Files.createFile(Paths.get(dorisHome, "fe", "mocked", "TimestampTest-" + uuid.toString())).toFile();
        if (LOG.isDebugEnabled()) {
            LOG.debug("createTmpFile path {}", testFile.getAbsolutePath());
        }
        testFiles.add(testFile.getAbsolutePath());
        return testFile.getAbsolutePath();
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (String testFile : testFiles) {
            LOG.info("delete testFile path {}", testFile);
            Files.deleteIfExists(Paths.get(testFile));
        }
    }

    // @Test
    @RepeatedTest(1)
    public void testSerialization() throws Exception {
        Timestamp timestamp = new Timestamp();
        long ts = timestamp.getTimestamp();
        Assertions.assertTrue(ts > 0);

        File testFile = new File(createTestFile());
        DataOutputStream out = new DataOutputStream(new FileOutputStream(testFile));
        timestamp.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(testFile));
        Thread.sleep(1000);
        Timestamp timestamp2 = Timestamp.read(in);

        Assertions.assertEquals(ts, timestamp2.getTimestamp());
        Assertions.assertEquals("" + ts, timestamp2.toString());
    }
}
