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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class MTMVUtilTest {
    @Test
    public void testGetExprTimeSec() throws AnalysisException {
        LiteralExpr expr = new DateLiteral("2020-01-01");
        long exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.empty());
        Assert.assertEquals(1577808000L, exprTimeSec);
        expr = new StringLiteral("2020-01-01");
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.of("%Y-%m-%d"));
        Assert.assertEquals(1577808000L, exprTimeSec);
        expr = new IntLiteral(20200101);
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.of("%Y%m%d"));
        Assert.assertEquals(1577808000L, exprTimeSec);
        expr = new DateLiteral(Type.DATE, true);
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.empty());
        Assert.assertEquals(253402185600L, exprTimeSec);
    }
}
