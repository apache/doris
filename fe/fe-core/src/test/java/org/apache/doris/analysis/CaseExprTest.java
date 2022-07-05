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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.List;

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

    @Test
    public void testTypeCast(
            @Injectable Expr caseExpr,
            @Injectable Expr whenExpr,
            @Injectable Expr thenExpr,
            @Injectable Expr elseExpr) throws Exception {
        CaseWhenClause caseWhenClause = new CaseWhenClause(whenExpr, thenExpr);
        List<CaseWhenClause> caseWhenClauseList = Lists.newArrayList();
        caseWhenClauseList.add(caseWhenClause);
        CaseExpr expr = new CaseExpr(caseExpr, caseWhenClauseList, elseExpr);
        Class<?> clazz = Class.forName("org.apache.doris.catalog.ScalarType");
        Constructor<?> constructor = clazz.getDeclaredConstructor(PrimitiveType.class);
        constructor.setAccessible(true);
        ScalarType intType = (ScalarType) constructor.newInstance(PrimitiveType.INT);
        ScalarType tinyIntType = (ScalarType) constructor.newInstance(PrimitiveType.TINYINT);
        new Expectations() {
            {
                caseExpr.getType();
                result = intType;

                whenExpr.getType();
                result = ScalarType.createType(PrimitiveType.INT);
                whenExpr.castTo(intType);
                times = 0;

                thenExpr.getType();
                result = tinyIntType;
                thenExpr.castTo(tinyIntType);
                times = 0;

                elseExpr.getType();
                result = ScalarType.createType(PrimitiveType.TINYINT);
                elseExpr.castTo(tinyIntType);
                times = 0;
            }
        };
        Analyzer analyzer = new Analyzer(null, null);
        expr.analyzeImpl(analyzer);
    }
}
