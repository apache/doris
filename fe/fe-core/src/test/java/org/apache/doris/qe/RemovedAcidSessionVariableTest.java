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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

class RemovedAcidSessionVariableTest {
    @Test
    void removedAcidVariableRemainsANoOpForSessionInitialization() throws Exception {
        SessionVariable variables = new SessionVariable();
        for (String name : new String[] {"skip_checking_acid_version_file",
                "skip_checking_acid_version_file".toUpperCase(Locale.ROOT)}) {
            for (boolean value : new boolean[] {true, false}) {
                SetVar setVar = new SetVar(SetType.SESSION, name, new BoolLiteral(value));
                Assertions.assertDoesNotThrow(() -> VariableMgr.setVar(variables, setVar));
                Assertions.assertDoesNotThrow(() -> VariableMgr.setVarForNonMasterFE(variables, setVar));
            }
            VariableExpr expression = new VariableExpr(name);
            VariableMgr.fillValue(variables, expression);
            Assertions.assertEquals(Type.VARCHAR, expression.getType());
            Assertions.assertEquals("", VariableMgr.getValue(variables, expression));
            Literal literal = VariableMgr.getLiteral(variables, name, SetType.SESSION);
            Assertions.assertNotNull(literal);
            Assertions.assertEquals("", literal.getStringValue());
            Assertions.assertNull(VariableMgr.getVarContext(name));
        }
        Assertions.assertFalse(VariableMgr.toMap(variables).containsKey("skip_checking_acid_version_file"));
    }
}
