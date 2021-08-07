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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.Function;
import org.apache.doris.common.AnalysisException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * rewrite alias function to real function
 */
public class RewriteAliasFunctionRule implements ExprRewriteRule{
    private final static Logger LOG = LogManager.getLogger(RewriteAliasFunctionRule.class);
    public static RewriteAliasFunctionRule INSTANCE = new RewriteAliasFunctionRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (expr instanceof FunctionCallExpr) {
            Function fn = expr.getFn();
            if (fn instanceof AliasFunction) {
                return ((FunctionCallExpr) expr).rewriteExpr();
            }

        }
        return expr;
    }
}
