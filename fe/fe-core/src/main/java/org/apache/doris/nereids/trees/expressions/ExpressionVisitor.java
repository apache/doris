// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;

/**
 * Use the visitor pattern to iterate over all expressions for expression rewriting.
 */
public abstract class ExpressionVisitor<R, C> {

    public abstract R visit(Alias alias, C context);

    public abstract R visit(ComparisonPredicate cp, C context);

    public abstract R visit(EqualTo equalTo, C context);

    public abstract R visit(GreaterThan greaterThan, C context);

    public abstract R visit(GreaterThanEqual greaterThanEqual, C context);

    public abstract R visit(LessThan lessThan, C context);

    public abstract R visit(LessThanEqual lessThanEqual, C context);

    public abstract R visit(NamedExpression namedExpression, C context);

    public abstract R visit(Not not, C context);

    public abstract R visit(NullSafeEqual nullSafeEqual, C context);

    public abstract R visit(SlotReference slotReference, C context);

    public abstract R visit(Literal literal, C context);

    public abstract R visit(FunctionCall function, C context);

    public abstract R visit(BetweenPredicate betweenPredicate, C context);

    public abstract R visit(CompoundPredicate compoundPredicate, C context);

    public abstract R visit(Arithmetic arithmetic, C context);


    /* ********************************************************************************************
     * Unbound expressions
     * ********************************************************************************************/

    public abstract R visit(UnboundAlias unboundAlias, C context);

    public abstract R visit(UnboundSlot unboundSlot, C context);

    public abstract R visit(UnboundStar unboundStar, C context);
}
