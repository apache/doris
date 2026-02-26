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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WorkloadSchedPolicyMgrTest {

    @Mocked
    private Env env;

    @Injectable
    private EditLog editLog;

    @Before
    public void setUp() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
    }

    @Test
    public void testCheckPolicyCondition() {
        WorkloadSchedPolicyMgr mgr = new WorkloadSchedPolicyMgr();

        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
            conditionMetas.add(new WorkloadConditionMeta("be_scan_rows", ">", "1000"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

            mgr.createWorkloadSchedPolicy("policy1", false, conditionMetas, actionMetas, null);

        } catch (UserException e) {
            Assert.fail("Should not throw exception for mixed USERNAME and BE metrics: " + e.getMessage());
        }
    }
}
