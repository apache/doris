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

package org.apache.doris.load.loadv2;

import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProgressManagerTest {
    @Test
    public void testUnregisteredAndLateReportsDoNotLog() {
        ProgressManager manager = new ProgressManager();
        TUniqueId queryId = new TUniqueId(1, 1);
        TUniqueId fragmentId = new TUniqueId(2, 1);
        try (TestLogAppender appender = TestLogAppender.attach(ProgressManager.class)) {
            // INSERT has a load job ID but does not register scan progress.
            manager.addTotalScanNums("insert", 8);
            manager.updateProgress("insert", queryId, fragmentId, 8);
            Assertions.assertEquals("Unknown id: insert", manager.getProgressInfo("insert", true));

            manager.registerProgress("load", 8);
            manager.removeProgress("load");
            manager.updateProgress("load", queryId, fragmentId, 8);
            Assertions.assertEquals("Unknown id: load", manager.getProgressInfo("load", true));
            Assertions.assertFalse(appender.contains(Level.WARN, "missing meta information"));
            Assertions.assertFalse(appender.contains(Level.DEBUG, "missing meta information"));
        }
    }

    @Test
    public void testRegisteredProgressAcrossQueriesAndRetry() {
        ProgressManager manager = new ProgressManager();
        manager.registerProgressSimple("load");
        manager.addTotalScanNums("load", 8);
        TUniqueId firstQuery = new TUniqueId(1, 1);
        TUniqueId secondQuery = new TUniqueId(1, 2);
        TUniqueId firstFragment = new TUniqueId(2, 1);
        TUniqueId secondFragment = new TUniqueId(2, 2);
        manager.updateProgress("load", firstQuery, firstFragment, 2);
        manager.updateProgress("load", firstQuery, firstFragment, 3);
        manager.updateProgress("load", firstQuery, secondFragment, 1);
        manager.updateProgress("load", secondQuery, firstFragment, 2);
        Assertions.assertEquals("75.00% (6/8)", manager.getProgressInfo("load", false));
        manager.updateProgress("load", secondQuery, firstFragment, 4);
        Assertions.assertEquals("99.99% (8/8)", manager.getProgressInfo("load", false));
        Assertions.assertEquals("100.00% (8/8)", manager.getProgressInfo("load", true));

        // A retry resets both the total and the per-query fragment counters.
        manager.registerProgressSimple("load");
        manager.addTotalScanNums("load", 4);
        Assertions.assertEquals("0.00% (0/4)", manager.getProgressInfo("load", false));
        manager.updateProgress("load", secondQuery, firstFragment, 4);
        Assertions.assertEquals("100.00% (4/4)", manager.getProgressInfo("load", true));
    }
}
