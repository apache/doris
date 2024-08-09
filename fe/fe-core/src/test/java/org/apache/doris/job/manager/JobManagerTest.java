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

package org.apache.doris.job.manager;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;

public class JobManagerTest {
    @Test
    public void testJobAuth() throws IOException, AnalysisException {
        UserIdentity user1 = new UserIdentity("testJobAuthUser", "%");
        user1.analyze();
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = TestWithFeService.createCtx(user1, "%");
            }
        };
        JobManager manager = new JobManager();
        HashSet<String> tableNames = Sets.newHashSet();
        try {
            // should check db auth
            manager.checkJobAuth("ctl1", "db1", tableNames);
            throw new RuntimeException("should exception");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Admin_priv,Load_priv"));
            Assert.assertTrue(e.getMessage().contains("db1"));
        }
        tableNames.add("table1");
        try {
            // should check db auth
            manager.checkJobAuth("ctl1", "db1", tableNames);
            throw new RuntimeException("should exception");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Admin_priv,Load_priv"));
            Assert.assertTrue(e.getMessage().contains("table1"));
        }
    }
}
