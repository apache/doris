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

class SessionVariableAffinityTest {

    @Test
    void testPreferredResourceGroupValidationAcceptsValidTagName() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_RESOURCE_GROUP, new StringLiteral("group_a")));

        Assertions.assertEquals("group_a", sessionVariable.getPreferredResourceGroup());
    }

    @Test
    void testPreferredResourceGroupValidationRejectsInvalidTagName() {
        SessionVariable sessionVariable = new SessionVariable();

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                        SessionVariable.PREFERRED_RESOURCE_GROUP, new StringLiteral("bad tag"))));

        Assertions.assertTrue(exception.getMessage().contains("preferred_resource_group value is invalid"));
    }

    @Test
    void testResourceGroupSelectPolicyRejectsInvalidToken() {
        SessionVariable sessionVariable = new SessionVariable();

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                        SessionVariable.RESOURCE_GROUP_SELECT_POLICY, new StringLiteral("auto"))));

        Assertions.assertTrue(exception.getMessage().contains("resource_group_select_policy value is invalid"));
    }

    @Test
    void testResourceGroupSelectPolicyDefaultIsPreferLocalInShowVariables() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(
                SessionVariable.RESOURCE_GROUP_SELECT_POLICY, false);

        List<List<String>> rows = VariableMgr.dump(SetType.SESSION, sessionVariable, matcher);

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(SessionVariable.RESOURCE_GROUP_SELECT_POLICY, rows.get(0).get(0));
        Assertions.assertEquals("prefer_local", rows.get(0).get(1));
    }

    @Test
    void testSettingPreferredToEmptyStringIsAllowed() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_RESOURCE_GROUP, new StringLiteral("group_a")));

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.PREFERRED_RESOURCE_GROUP, new StringLiteral("")));

        Assertions.assertEquals("", sessionVariable.getPreferredResourceGroup());
    }

    @Test
    void testLoadLocalAffinityDefaultIsOffAndCanBeEnabled() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();

        Assertions.assertFalse(sessionVariable.isEnableLoadLocalAffinity());

        VariableMgr.setVar(sessionVariable, new SetVar(SetType.SESSION,
                SessionVariable.ENABLE_LOAD_LOCAL_AFFINITY, new StringLiteral("true")));

        Assertions.assertTrue(sessionVariable.isEnableLoadLocalAffinity());
    }
}
