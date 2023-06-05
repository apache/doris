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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

public class SetUserDefinedVar extends SetVar {
    public SetUserDefinedVar(String variable, Expr value) {
        super(SetType.USER, variable, value, SetVarType.SET_USER_DEFINED_VAR);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException  {
        Expr expression = getValue();
        if (expression instanceof NullLiteral) {
            setResult(NullLiteral.create(ScalarType.NULL));
        } else if (expression instanceof LiteralExpr) {
            setResult((LiteralExpr) expression);
        } else {
            throw new AnalysisException("Unsupported to set the non-literal for user defined variables.");
        }
    }
}
