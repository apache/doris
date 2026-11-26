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

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.spi.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class VariableMgrExportTest {
    @Test
    void exportsCurrentValuesWithoutChangingPreviousSnapshots() throws Exception {
        SessionVariable variables = new SessionVariable();
        variables.batchSize = 100;
        variables.enableProfile = false;
        Map<String, String> first = VariableMgr.toMap(variables);

        VariableMgr.setVar(variables, new SetVar(SetType.SESSION, SessionVariable.BATCH_SIZE,
                new StringLiteral("200")));
        variables.enableProfile = true;
        Map<String, String> second = VariableMgr.toMap(variables);
        Assertions.assertEquals("100", first.get(SessionVariable.BATCH_SIZE));
        Assertions.assertEquals("false", first.get(SessionVariable.ENABLE_PROFILE));
        Assertions.assertEquals("200", second.get(SessionVariable.BATCH_SIZE));
        Assertions.assertEquals("true", second.get(SessionVariable.ENABLE_PROFILE));
    }

    @Test
    void exportsAliasesAndExcludesRemovedAndInvisibleVariables() throws Exception {
        SessionVariable variables = new SessionVariable();
        VariableMgr.setVar(variables, new SetVar(SetType.SESSION, "enable_pushdown_string_minmax",
                new StringLiteral("false")));
        Map<String, String> values = VariableMgr.toMap(variables);
        Assertions.assertEquals("false", values.get("enable_pushdown_string_minmax"));
        Assertions.assertEquals("false", values.get(SessionVariable.FORCE_PUSHDOWN_ZONEMAP_MINMAX));
        Assertions.assertFalse(values.containsKey(SessionVariable.ENABLE_LOCAL_EXCHANGE));
        Assertions.assertFalse(values.containsKey(SessionVariable.ENABLE_NEREIDS_PLANNER));
    }

    @Test
    void rereadsGlobalValuesOnEachExport() {
        int original = GlobalVariable.lowerCaseTableNames;
        try {
            GlobalVariable.lowerCaseTableNames = 1;
            Map<String, String> first = VariableMgr.toMap(new SessionVariable());
            GlobalVariable.lowerCaseTableNames = 2;
            Map<String, String> second = VariableMgr.toMap(new SessionVariable());
            Assertions.assertEquals("1", first.get(GlobalVariable.LOWER_CASE_TABLE_NAMES));
            Assertions.assertEquals("2", second.get(GlobalVariable.LOWER_CASE_TABLE_NAMES));
        } finally {
            GlobalVariable.lowerCaseTableNames = original;
        }
    }

    @Test
    void connectorSessionsObserveTheirOwnCurrentSettings() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().batchSize = 100;
        ConnectorSession first = ConnectorSessionBuilder.from(context).build();
        context.getSessionVariable().batchSize = 200;
        ConnectorSession second = ConnectorSessionBuilder.from(context).build();
        Assertions.assertEquals(100, first.getProperty(SessionVariable.BATCH_SIZE, Integer.class));
        Assertions.assertEquals(200, second.getProperty(SessionVariable.BATCH_SIZE, Integer.class));
    }
}
