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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.List;

/**
 * Expression and it's index mapping
 */
public class ExpressionIndexMapping extends Mapping {
    private final Multimap<Expression, Integer> expressionIndexMapping;

    public ExpressionIndexMapping(Multimap<Expression, Integer> expressionIndexMapping) {
        this.expressionIndexMapping = expressionIndexMapping;
    }

    public Multimap<Expression, Integer> getExpressionIndexMapping() {
        return expressionIndexMapping;
    }

    public static ExpressionIndexMapping generate(List<? extends Expression> expressions) {
        Multimap<Expression, Integer> expressionIndexMapping = ArrayListMultimap.create();
        for (int i = 0; i < expressions.size(); i++) {
            expressionIndexMapping.put(expressions.get(i), i);
        }
        return new ExpressionIndexMapping(expressionIndexMapping);
    }
}
