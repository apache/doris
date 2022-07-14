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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalDataSource;

import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FunctionCallExprTest {
    private static final String internalCtl = InternalDataSource.INTERNAL_DS_NAME;

    @Test
    public void testDecimalFunction(@Mocked Analyzer analyzer) throws AnalysisException {
        new MockUp<SlotRef>(SlotRef.class) {
            @Mock
            public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
                return;
            }
        };
        Expr argExpr = new SlotRef(new TableName(internalCtl, "db", "table"), "c0");
        FunctionCallExpr functionCallExpr;
        boolean hasException = false;
        Type res;
        ImmutableList<String> sameTypeFunction = ImmutableList.<String>builder()
                .add("min").add("max").add("lead").add("lag")
                .add("first_value").add("last_value").add("abs")
                .add("positive").add("negative").build();
        ImmutableList<String> widerTypeFunction = ImmutableList.<String>builder()
                .add("sum").add("avg").add("multi_distinct_sum").build();
        try {
            for (String func : sameTypeFunction) {
                Type argType = ScalarType.createDecimalType(9, 4);
                argExpr.setType(argType);
                functionCallExpr = new FunctionCallExpr(func, Arrays.asList(argExpr));
                functionCallExpr.setIsAnalyticFnCall(true);
                res = ScalarType.createDecimalType(9, 4);
                functionCallExpr.analyzeImpl(analyzer);
                Assert.assertEquals(functionCallExpr.type, res);
            }

            for (String func : widerTypeFunction) {
                Type argType = ScalarType.createDecimalType(9, 4);
                argExpr.setType(argType);
                functionCallExpr = new FunctionCallExpr(func, Arrays.asList(argExpr));
                res = ScalarType.createDecimalType(38, 4);
                functionCallExpr.analyzeImpl(analyzer);
                Assert.assertEquals(functionCallExpr.type, res);
            }

        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

}
