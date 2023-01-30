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

import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.load.ExportJob;
import org.apache.doris.task.ExportExportingTask;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

public class SessionVariablesTest extends TestWithFeService {

    private SessionVariable sessionVariable;
    private int numOfForwardVars;
    private ProfileManager profileManager = ProfileManager.getInstance();

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test_d");
        useDatabase("test_d");
        createTable("create table test_t1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        sessionVariable = new SessionVariable();
        Field[] fields = SessionVariable.class.getFields();
        for (Field f : fields) {
            VariableMgr.VarAttr varAttr = f.getAnnotation(VariableMgr.VarAttr.class);
            if (varAttr == null || !varAttr.needForward()) {
                continue;
            }
            numOfForwardVars++;
        }
    }

    @Test
    public void testForwardSessionVariables() {
        Map<String, String> vars = sessionVariable.getForwardVariables();
        Assertions.assertTrue(numOfForwardVars >= 6);
        Assertions.assertEquals(numOfForwardVars, vars.size());

        vars.put(SessionVariable.ENABLE_PROFILE, "true");
        sessionVariable.setForwardedSessionVariables(vars);
        Assertions.assertEquals(true, sessionVariable.enableProfile);
    }

    @Test
    public void testForwardQueryOptions() {
        TQueryOptions queryOptions = sessionVariable.getQueryOptionVariables();
        Assertions.assertTrue(queryOptions.isSetMemLimit());
        Assertions.assertFalse(queryOptions.isSetLoadMemLimit());
        Assertions.assertTrue(queryOptions.isSetQueryTimeout());

        queryOptions.setQueryTimeout(123);
        sessionVariable.setForwardedSessionVariables(queryOptions);
        Assertions.assertEquals(123, sessionVariable.getQueryTimeoutS());
    }

    @Test
    public void testEnableProfile() {
        try {
            SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt("set enable_profile=true", connectContext);
            SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
            setExecutor.execute();

            ExportStmt exportStmt = (ExportStmt)
                    parseAndAnalyzeStmt("EXPORT TABLE test_d.test_t1 TO \"file:///tmp/test_t1\"", connectContext);
            ExportJob job = new ExportJob(1234);
            job.setJob(exportStmt);

            new Expectations(job) {
                {
                    job.getState();
                    minTimes = 0;
                    result = ExportJob.JobState.EXPORTING;

                    job.getCoordList();
                    minTimes = 0;
                    result = Lists.newArrayList();
                }
            };

            new Expectations(profileManager) {
                {
                    profileManager.pushProfile((RuntimeProfile) any);
                    // if enable_profile=true, method pushProfile will be called once
                    times = 1;
                }
            };

            ExportExportingTask task = new ExportExportingTask(job);
            task.run();
            Assertions.assertTrue(job.isFinalState());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }

    }

    @Test
    public void testDisableProfile() {
        try {
            SetStmt setStmt = (SetStmt) parseAndAnalyzeStmt("set enable_profile=false", connectContext);
            SetExecutor setExecutor = new SetExecutor(connectContext, setStmt);
            setExecutor.execute();

            ExportStmt exportStmt = (ExportStmt)
                    parseAndAnalyzeStmt("EXPORT TABLE test_d.test_t1 TO \"file:///tmp/test_t1\"", connectContext);
            ExportJob job = new ExportJob(1234);
            job.setJob(exportStmt);

            new Expectations(job) {
                {
                    job.getState();
                    minTimes = 0;
                    result = ExportJob.JobState.EXPORTING;

                    job.getCoordList();
                    minTimes = 0;
                    result = Lists.newArrayList();
                }
            };

            new Expectations(profileManager) {
                {
                    profileManager.pushProfile((RuntimeProfile) any);
                    // if enable_profile=false, method pushProfile will not be called
                    times = 0;
                }
            };

            ExportExportingTask task = new ExportExportingTask(job);
            task.run();
            Assertions.assertTrue(job.isFinalState());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }

    }
}
