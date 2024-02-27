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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.junit.jupiter.api.Assertions;

public class PlanParseChecker extends ParseChecker {
    final Supplier<Plan> parsedSupplier;

    public PlanParseChecker(String sql) {
        super(sql);
        this.parsedSupplier = Suppliers.memoize(() -> PARSER.parseSingle(sql));
    }

    public PlanParseChecker matches(PatternDescriptor<? extends Plan> patternDesc) {
        assertMatches(() -> MatchingUtils.topDownFindMatching(
                new Memo(null, parsedSupplier.get()).getRoot(), patternDesc.pattern));
        return this;
    }

    public PlanParseChecker matchesFromRoot(PatternDescriptor<? extends Plan> patternDesc) {
        assertMatches(() -> new GroupExpressionMatching(patternDesc.pattern,
                new Memo(null, parsedSupplier.get()).getRoot().getLogicalExpression())
                .iterator().hasNext());
        return this;
    }

    public <T extends Throwable> ExceptionChecker assertThrows(Class<T> expectedType) {
        return new ExceptionChecker(Assertions.assertThrows(expectedType, parsedSupplier::get));
    }

    public <T extends Throwable> ExceptionChecker assertThrowsExactly(Class<T> expectedType) {
        return new ExceptionChecker(Assertions.assertThrowsExactly(expectedType, parsedSupplier::get));
    }

    private void assertMatches(Supplier<Boolean> assertResultSupplier) {
        Assertions.assertTrue(assertResultSupplier.get(),
                () -> "pattern not match,\ninput SQL:\n" + sql
                        + "\n, parsed plan :\n" + parsedSupplier.get().treeString() + "\n"
        );
    }
}
