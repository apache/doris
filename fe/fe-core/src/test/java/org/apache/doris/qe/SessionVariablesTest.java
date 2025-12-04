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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

public class SessionVariablesTest extends TestWithFeService {

    private SessionVariable sessionVariable;
    private int numOfForwardVars;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test_d");
        useDatabase("test_d");
        createTable("create table test_t1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");

        sessionVariable = new SessionVariable();
        Field[] fields = SessionVariable.class.getDeclaredFields();
        for (Field f : fields) {
            VariableMgr.VarAttr varAttr = f.getAnnotation(VariableMgr.VarAttr.class);
            if (varAttr == null || !(varAttr.needForward() || varAttr.affectQueryResult())) {
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
        Assertions.assertTrue(sessionVariable.enableProfile);
    }

    @Test
    public void testCloneSessionVariablesWithSessionOriginValueNotEmpty() throws NoSuchFieldException {
        Field txIsolation = SessionVariable.class.getField("txIsolation");
        SessionVariableField txIsolationSessionVariableField = new SessionVariableField(txIsolation);
        sessionVariable.addSessionOriginValue(txIsolationSessionVariableField, "test");

        SessionVariable sessionVariableClone = VariableMgr.cloneSessionVariable(sessionVariable);

        Assertions.assertEquals("test",
                sessionVariableClone.getSessionOriginValue().get(txIsolationSessionVariableField));
    }

    @Test
    public void testSetVarInHint() {
        String sql = "insert into test_t1 select /*+ set_var(enable_nereids_dml_with_pipeline=false)*/ * from test_t1 where enable_nereids_dml_with_pipeline=true";
        new NereidsParser().parseSQL(sql);
        Assertions.assertEquals(false, connectContext.getSessionVariable().enableNereidsDmlWithPipeline);
    }
}
