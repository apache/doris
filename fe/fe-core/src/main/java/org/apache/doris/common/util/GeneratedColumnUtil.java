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

package org.apache.doris.common.util;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class GeneratedColumnUtil {
    public static class ExprAndname {
        private Expr expr;
        private String name;

        public ExprAndname(Expr expr, String name) {
            this.expr = expr;
            this.name = name;
        }

        public Expr getExpr() {
            return expr;
        }

        public void setExpr(Expr expr) {
            this.expr = expr;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static void rewriteColumns(List<ExprAndname> exprAndnames) {
        Map<String, Expr> nameToExprMap = Maps.newHashMap();
        for (ExprAndname exprAndname : exprAndnames) {
            if (exprAndname.getExpr() instanceof SlotRef) {
                String columnName = ((SlotRef) exprAndname.getExpr()).getColumnName();
                if (nameToExprMap.containsKey(columnName)) {
                    exprAndname.setExpr(nameToExprMap.get(columnName));
                }
            } else {
                ExprUtil.recursiveRewrite(exprAndname.getExpr(), nameToExprMap);
            }
            nameToExprMap.put(exprAndname.getName(), exprAndname.getExpr());
        }
    }
}
