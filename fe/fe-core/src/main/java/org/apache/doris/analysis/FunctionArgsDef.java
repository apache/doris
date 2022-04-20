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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FunctionArgsDef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import java.util.List;

public class FunctionArgsDef {
    private final List<TypeDef> argTypeDefs;
    private final boolean isVariadic;

    // set after analyze
    private Type[] argTypes;

    public FunctionArgsDef(List<TypeDef> argTypeDefs, boolean isVariadic) {
        this.argTypeDefs = argTypeDefs;
        this.isVariadic = isVariadic;
    }

    public Type[] getArgTypes() { return argTypes; }
    public boolean isVariadic() { return isVariadic; }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        argTypes = new Type[argTypeDefs.size()];
        int i = 0;
        for (TypeDef typeDef : argTypeDefs) {
            typeDef.analyze(analyzer);
            argTypes[i++] = typeDef.getType();
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        int i = 0;
        for (TypeDef typeDef : argTypeDefs) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(typeDef.toString());
            i++;
        }
        if (isVariadic) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("...");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() { return toSql(); }
}
