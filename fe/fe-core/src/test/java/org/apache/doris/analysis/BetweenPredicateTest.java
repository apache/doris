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

import org.apache.doris.common.AnalysisException;

import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BetweenPredicateTest {
    @Mocked Analyzer analyzer;

    @Test
    public void testWithCompareAndBoundSubquery(@Injectable Subquery compareExpr,
                                 @Injectable Subquery lowerBound,
                                 @Injectable Expr upperBound) {
        BetweenPredicate betweenPredicate = new BetweenPredicate(compareExpr, lowerBound, upperBound, false);
        try {
            betweenPredicate.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }

    @Test
    public void testWithBoundSubquery(@Injectable Expr compareExpr,
                                      @Injectable Subquery lowerBound,
                                      @Injectable Subquery upperBound) {
        BetweenPredicate betweenPredicate = new BetweenPredicate(compareExpr, lowerBound, upperBound, false);
        try {
            betweenPredicate.analyzeImpl(analyzer);
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }
}
