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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.SmallIntType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Set;

class PredicatePropagationTest {
    private final SlotReference a = new SlotReference("a", SmallIntType.INSTANCE);
    private final SlotReference b = new SlotReference("b", BigIntType.INSTANCE);
    private final SlotReference c = new SlotReference("c", BigIntType.INSTANCE);

    @Test
    void equal() {
        Set<Expression> exprs = ImmutableSet.of(new EqualTo(a, b), new EqualTo(a, Literal.of(1)));
        Set<Expression> inferExprs = PredicatePropagation.infer(exprs);
        System.out.println(inferExprs);
    }

    @Test
    void in() {
        Set<Expression> exprs = ImmutableSet.of(new EqualTo(a, b), new InPredicate(a, ImmutableList.of(Literal.of(1))));
        Set<Expression> inferExprs = PredicatePropagation.infer(exprs);
        System.out.println(inferExprs);
    }

    @Test
    void inferSlotEqual() {
        Set<Expression> exprs = ImmutableSet.of(new EqualTo(a, b), new EqualTo(a, c));
        Set<Expression> inferExprs = PredicatePropagation.infer(exprs);
        System.out.println(inferExprs);
    }

    @Test
    void inferComplex0() {
        Set<Expression> exprs = ImmutableSet.of(new EqualTo(a, b), new EqualTo(a, c), new GreaterThan(a, Literal.of(1)));
        Set<Expression> inferExprs = PredicatePropagation.infer(exprs);
        System.out.println(inferExprs);
    }
}
