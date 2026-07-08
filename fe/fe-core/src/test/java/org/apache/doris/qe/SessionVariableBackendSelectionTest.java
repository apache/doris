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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class SessionVariableBackendSelectionTest {

    @Test
    void testPreferredBackendSelectionKeyValidationAcceptsValidKey() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_BACKEND_SELECTION_KEY, new StringLiteral("group_a")));

        Assertions.assertEquals("group_a", sessionVariable.getPreferredBackendSelectionKey());
    }

    @Test
    void testPreferredBackendSelectionKeyValidationRejectsInvalidKey() {
        SessionVariable sessionVariable = new SessionVariable();

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                        SessionVariable.PREFERRED_BACKEND_SELECTION_KEY, new StringLiteral("bad tag"))));

        Assertions.assertTrue(exception.getMessage().contains("preferred_backend_selection_key value is invalid"));
    }

    @Test
    void testBackendSelectionModeRejectsInvalidToken() {
        SessionVariable sessionVariable = new SessionVariable();

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                        SessionVariable.BACKEND_SELECTION_MODE, new StringLiteral("auto"))));

        Assertions.assertTrue(exception.getMessage().contains("backend_selection_mode value is invalid"));
    }

    @Test
    void testBackendSelectionModeDefaultIsPreferInShowVariables() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(
                SessionVariable.BACKEND_SELECTION_MODE, false);

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, sessionVariable, matcher);

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(SessionVariable.BACKEND_SELECTION_MODE, rows.get(0).get(0));
        Assertions.assertEquals("prefer", rows.get(0).get(1));
    }

    @Test
    void testSettingPreferredToEmptyStringIsAllowed() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_BACKEND_SELECTION_KEY, new StringLiteral("group_a")));

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_BACKEND_SELECTION_KEY, new StringLiteral("")));

        Assertions.assertEquals("", sessionVariable.getPreferredBackendSelectionKey());
    }

    @Test
    void testLoadBackendSelectionDefaultIsOffAndCanBeEnabled() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        Assertions.assertFalse(sessionVariable.isEnableLoadBackendSelection());

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.ENABLE_LOAD_BACKEND_SELECTION, new StringLiteral("true")));

        Assertions.assertTrue(sessionVariable.isEnableLoadBackendSelection());
    }
}
