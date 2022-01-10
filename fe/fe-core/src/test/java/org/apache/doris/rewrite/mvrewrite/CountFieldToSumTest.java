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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import org.apache.doris.rewrite.ExprRewriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class CountFieldToSumTest {

    @Test
    public void testCountDistinct(@Injectable Analyzer analyzer,
                                  @Injectable FunctionCallExpr functionCallExpr) {
        TableName tableName = new TableName("db1", "table1");
        SlotRef slotRef = new SlotRef(tableName,"c1");
        List<Expr>  params = Lists.newArrayList();
        params.add(slotRef);

        new Expectations() {
            {
                functionCallExpr.getFnName().getFunction();
                result = FunctionSet.COUNT;
                functionCallExpr.getChildren();
                result = params;
                functionCallExpr.getChild(0);
                result = slotRef;
                functionCallExpr.getParams().isDistinct();
                result = true;
            }
        };
        CountFieldToSum countFieldToSum = new CountFieldToSum();
        try {
            Expr rewrittenExpr = countFieldToSum.apply(functionCallExpr, analyzer, ExprRewriter.ClauseType.OTHER_CLAUSE);
            Assert.assertTrue(rewrittenExpr instanceof FunctionCallExpr);
            Assert.assertEquals(FunctionSet.COUNT, ((FunctionCallExpr) rewrittenExpr).getFnName().getFunction());
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
    }
}
