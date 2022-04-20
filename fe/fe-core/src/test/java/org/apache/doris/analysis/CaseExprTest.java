// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.apache.doris.analysis;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import com.clearspring.analytics.util.Lists;
import mockit.Expectations;
import mockit.Injectable;

public class CaseExprTest {

    @Test
    public void testIsNullable(@Injectable Expr caseExpr,
                               @Injectable Expr whenExpr,
                               @Injectable Expr thenExpr,
                               @Injectable Expr elseExpr) {
        // without case without else
        CaseWhenClause caseWhenClause = new CaseWhenClause(whenExpr, thenExpr);
        List<CaseWhenClause> caseWhenClauseList = Lists.newArrayList();
        caseWhenClauseList.add(caseWhenClause);
        CaseExpr caseExpr1 = new CaseExpr(null, caseWhenClauseList, null);
        new Expectations() {
            {
                thenExpr.isNullable();
                result = false;
            }
        };
        Assert.assertTrue(caseExpr1.isNullable());
        // with case without else
        CaseExpr caseExpr2 = new CaseExpr(caseExpr, caseWhenClauseList, null);
        new Expectations() {
            {
                thenExpr.isNullable();
                result = false;
            }
        };
        Assert.assertTrue(caseExpr2.isNullable());
        // with case with else
        CaseExpr caseExpr3 = new CaseExpr(caseExpr, caseWhenClauseList, elseExpr);
        new Expectations() {
            {
                thenExpr.isNullable();
                result = false;
                elseExpr.isNullable();
                result = true;
            }
        };
        Assert.assertTrue(caseExpr3.isNullable());
    }
}
