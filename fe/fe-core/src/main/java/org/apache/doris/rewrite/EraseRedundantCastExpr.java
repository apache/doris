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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FoldConstantsRule.java
// and modified by Doris

package org.apache.doris.rewrite;


import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This rule erase redundant cast between datev2/datetimev2 and date literal.
 *
 * for expression with pattern:
 * BinaryPredicate
 *  CAST (SlotRef(TYPE = DATEV2/DATETIMEV2), TYPE = DATETIME)
 *  DATELITERAL(TYPE = DATETIME)
 *
 * this rule will be applied.
 */
public class EraseRedundantCastExpr implements ExprRewriteRule {
    private static final Logger LOG = LogManager.getLogger(EraseRedundantCastExpr.class);

    public static EraseRedundantCastExpr INSTANCE = new EraseRedundantCastExpr();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        // BinaryPredicate
        //   CAST (SlotRef(TYPE = DATEV2/DATETIMEV2), TYPE = DATETIME)
        //   DATELITERAL(TYPE = DATETIME)
        if (!(expr instanceof BinaryPredicate) || !(expr.getChild(0) instanceof CastExpr)
                || !expr.getChild(0).getType().isDatetime() || !(expr.getChild(1)).getType().isDatetime()
                || !expr.getChild(0).getChild(0).getType().isDateV2OrDateTimeV2()
                || !(expr.getChild(0).getChild(0) instanceof SlotRef)
                || !(expr.getChild(1) instanceof DateLiteral)) {
            return expr;
        }

        if (!((DateLiteral) expr.getChild(1)).hasTimePart()
                || !(expr.getChild(1).getType().isDatetime() && expr.getChild(0).getChild(0).getType().isDateV2())) {
            expr.getChild(1).setType(expr.getChild(0).getChild(0).getType());
            expr.setChild(0, expr.getChild(0).getChild(0));
        }
        return expr;
    }
}
