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

package org.apache.doris.rewrite.mvrewrite;

import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

/**
 * Rewrite count(k1) to sum(mv_count_k1) when MV Column exists.
 * For example:
 * Table: (k1 int ,k2 varchar)
 * MV: (k1 int, mv_count_k2 bigint sum)
 * mv_count_k1 = case when k2 is null then 0 else 1
 * Query: select k1, count(k2) from table group by k1
 * Rewritten query: select k1, sum(mv_count_k2) from table group by k1
 */
public class CountFieldToSum {

    public static Expr slotToCaseWhen(Expr expr) throws AnalysisException {
        return new CaseExpr(null, Lists.newArrayList(new CaseWhenClause(
                new IsNullPredicate(expr, false),
                new IntLiteral(0, Type.BIGINT))), new IntLiteral(1, Type.BIGINT));
    }
}
