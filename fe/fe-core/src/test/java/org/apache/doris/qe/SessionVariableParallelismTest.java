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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.UserPropertyInfo;
import org.apache.doris.mysql.privilege.UserPropertyMgr;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;

public class SessionVariableParallelismTest {
    private static final String USER = "parallelism_test";
    private UserPropertyMgr propertyMgr;
    private SessionVariable sessionVariable;

    @BeforeEach
    public void setUp() throws Exception {
        propertyMgr = new UserPropertyMgr();
        propertyMgr.addUserResource(USER);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(auth.getParallelFragmentExecInstanceNum(USER))
                .thenAnswer(invocation -> propertyMgr.getParallelFragmentExecInstanceNum(USER));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getInternalCatalog()).thenReturn(new InternalCatalog());
        ConnectContext context = new ConnectContext();
        context.setEnv(env);
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(USER, "%"));
        context.setThreadLocalInfo();
        sessionVariable = context.getSessionVariable();
        sessionVariable.setPipelineTaskNum("8");
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testAutomaticParallelismLimit() throws Exception {
        sessionVariable.setPipelineTaskNum("0");
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        SystemInfoService systemInfo = new SystemInfoService();
        systemInfo.addBackend(backend);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            // Executor report, max_instance_num, effective parallelism.
            for (int[] values : new int[][] {{1024, 1024, 256}, {1024, 64, 64}, {1024, 256, 256},
                    {510, 1024, 255}, {511, 1024, 256}, {512, 1024, 256}, {513, 1024, 256},
                    {8, 1024, 4}, {9, 1024, 5}}) {
                backend.setPipelineExecutorSize(values[0]);
                sessionVariable.maxInstanceNum = values[1];
                assertEffectiveParallelism(values[2]);
            }

            // A positive user property still takes precedence over automatic sizing.
            propertyMgr.updateUserProperty(USER, Collections.singletonList(
                    Pair.of("parallel_fragment_exec_instance_num", "7")), false);
            assertEffectiveParallelism(7);
        }
    }

    @Test
    public void testHistoricalSessionParallelismLimit() throws Exception {
        for (int value : new int[] {257, 2000, Integer.MAX_VALUE}) {
            sessionVariable.readFromJson("{\"parallel_pipeline_task_num\":" + value + "}");
            assertEffectiveParallelism(256);
        }
        sessionVariable.setPipelineTaskNum("8");
        assertEffectiveParallelism(8);
    }

    @Test
    public void testHistoricalConcurrencyHintRestoration() throws Exception {
        for (String variable : new String[] {SessionVariable.PARALLEL_PIPELINE_TASK_NUM,
                SessionVariable.COLOCATE_MAX_PARALLEL_NUM, SessionVariable.MAX_SCANNERS_CONCURRENCY,
                SessionVariable.MAX_FILE_SCANNERS_CONCURRENCY, SessionVariable.MIN_SCANNERS_CONCURRENCY,
                SessionVariable.MIN_FILE_SCANNERS_CONCURRENCY, SessionVariable.PARALLEL_SCAN_MAX_SCANNERS_COUNT,
                SessionVariable.SEND_BATCH_PARALLELISM, SessionVariable.LOAD_STREAM_PER_NODE}) {
            for (int value : new int[] {257, 2000, Integer.MAX_VALUE}) {
                assertHistoricalHintRestoration(variable, value, 256);
            }
            assertHistoricalHintRestoration(variable, 256, 256);
            assertHistoricalHintRestoration(variable, 8, 8);
        }
    }

    @Test
    public void testHistoricalConcurrencyDefaultValues() throws Exception {
        assertHistoricalHintRestoration(SessionVariable.PARALLEL_PIPELINE_TASK_NUM, 0, 0);
        for (String variable : new String[] {SessionVariable.COLOCATE_MAX_PARALLEL_NUM,
                SessionVariable.LOAD_STREAM_PER_NODE}) {
            assertHistoricalHintRestoration(variable, 0, 1);
            assertHistoricalHintRestoration(variable, -1, 1);
        }
        for (String variable : new String[] {SessionVariable.MAX_SCANNERS_CONCURRENCY,
                SessionVariable.MAX_FILE_SCANNERS_CONCURRENCY, SessionVariable.MIN_SCANNERS_CONCURRENCY,
                SessionVariable.MIN_FILE_SCANNERS_CONCURRENCY, SessionVariable.PARALLEL_SCAN_MAX_SCANNERS_COUNT,
                SessionVariable.SEND_BATCH_PARALLELISM}) {
            for (int value : new int[] {0, -1, Integer.MIN_VALUE}) {
                assertHistoricalHintRestoration(variable, value, value);
            }
        }
    }

    private void assertHistoricalHintRestoration(String variable, int value, int expected) throws Exception {
        SessionVariable fromJson = new SessionVariable();
        fromJson.readFromJson("{\"" + variable + "\":" + value + "}");
        assertHintRestoration(fromJson, variable, expected);

        SessionVariable fromMap = new SessionVariable();
        fromMap.readFromMap(Collections.singletonMap(variable, Integer.toString(value)));
        assertHintRestoration(fromMap, variable, expected);

        SessionVariable forwarded = new SessionVariable();
        if (forwarded.getForwardVariables().containsKey(variable)) {
            forwarded.setForwardedSessionVariables(Collections.singletonMap(variable, Integer.toString(value)));
            assertHintRestoration(forwarded, variable, expected);
        }
    }

    private void assertHintRestoration(SessionVariable restored, String variable, int expected) throws Exception {
        Assertions.assertTrue(restored.setVarOnce(variable, "8"));
        VariableMgr.revertSessionValue(restored);
        Assertions.assertEquals(expected, VariableMgr.getVarContext(variable).getField().getInt(restored), variable);
    }

    @Test
    public void testUserPropertyPrecedenceAndReset() throws Exception {
        assertEffectiveParallelism(8);
        for (int value : new int[] {1, 256, 0, -1, Integer.MIN_VALUE}) {
            propertyMgr.updateUserProperty(USER, Collections.singletonList(
                    Pair.of("parallel_fragment_exec_instance_num", Integer.toString(value))), false);
            assertEffectiveParallelism(value > 0 ? value : 8);
        }
    }

    @Test
    public void testHistoricalJournalParallelism() throws Exception {
        for (int value : new int[] {257, 2000, Integer.MAX_VALUE}) {
            UserPropertyInfo info = new UserPropertyInfo(USER, Collections.singletonList(
                    Pair.of("parallel_fragment_exec_instance_num", Integer.toString(value))));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            info.write(new DataOutputStream(bytes));
            UserPropertyInfo restored = UserPropertyInfo.read(
                    new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            propertyMgr.updateUserProperty(restored.getUser(), restored.getProperties(), true);
            Assertions.assertEquals(value, propertyMgr.getParallelFragmentExecInstanceNum(USER));
            assertEffectiveParallelism(256);

            bytes.reset();
            propertyMgr.write(new DataOutputStream(bytes));
            propertyMgr = UserPropertyMgr.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            assertEffectiveParallelism(256);
        }
    }

    @Test
    public void testHistoricalImageParallelism() throws Exception {
        // Cover both the current image field names and their legacy aliases.
        for (String[] fields : new String[][] {{"cp", "pfei"},
                {"commonProperties", "parallelFragmentExecInstanceNum"}}) {
            for (int value : new int[] {257, 2000, Integer.MAX_VALUE}) {
                String json = String.format("{\"propertyMap\":{\"%s\":{\"qu\":\"%s\",\"%s\":{\"%s\":%d}}}}",
                        USER, USER, fields[0], fields[1], value);
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                Text.writeString(new DataOutputStream(bytes), json);
                propertyMgr = UserPropertyMgr.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
                Assertions.assertEquals(value, propertyMgr.getParallelFragmentExecInstanceNum(USER));
                assertEffectiveParallelism(256);
            }
        }
    }

    private void assertEffectiveParallelism(int expected) {
        Assertions.assertEquals(expected, sessionVariable.getParallelExecInstanceNum(""));
        Assertions.assertEquals(expected, sessionVariable.toThrift().getParallelInstance());
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.RANDOM);
        Assertions.assertEquals(expected, fragment.getParallelExecNum());
    }
}
