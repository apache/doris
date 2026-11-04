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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class ExpressionUtilsTraversalTest {
    @Test
    void visitsEachNodeOnceWhenNoNodeMatches() {
        Expression expression = new Add(new IntegerLiteral(1),
                new Add(new IntegerLiteral(2), new IntegerLiteral(3)));
        AtomicInteger visits = new AtomicInteger();
        Assertions.assertFalse(ExpressionUtils.deepAnyMatch(ImmutableList.of(expression), node -> {
            visits.incrementAndGet();
            return false;
        }));
        Assertions.assertEquals(5, visits.get());
        visits.set(0);
        Assertions.assertTrue(ExpressionUtils.deepNoneMatch(ImmutableList.of(expression), node -> {
            visits.incrementAndGet();
            return false;
        }));
        Assertions.assertEquals(5, visits.get());
        Assertions.assertTrue(ExpressionUtils.deepAnyMatch(ImmutableList.of(expression),
                node -> node.equals(new IntegerLiteral(3))));
        Assertions.assertFalse(ExpressionUtils.deepNoneMatch(ImmutableList.of(expression),
                node -> node.equals(new IntegerLiteral(3))));
    }
}
