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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * given column s has data type: struct&lt;a: int, b: int&gt;, if exists struct_element(s, 2), we will rewrite
 * to struct_element(s, 'b')
 */
public class NormalizeStructElement implements ExpressionPatternRuleFactory {
    public static final NormalizeStructElement INSTANCE = new NormalizeStructElement();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(StructElement.class).then(NormalizeStructElement::normalize)
                        .toRule(ExpressionRuleType.NORMALIZE_STRUCT_ELEMENT)
        );
    }

    private static StructElement normalize(StructElement structElement) {
        Expression field = structElement.getArgument(1);
        if (field instanceof IntegerLikeLiteral) {
            int fieldIndex = ((Number) ((IntegerLikeLiteral) field).getValue()).intValue();
            StructType structType = (StructType) structElement.getArgument(0).getDataType();
            List<StructField> fields = structType.getFields();
            if (fieldIndex >= 0 && fieldIndex <= fields.size()) {
                return structElement.withChildren(
                        ImmutableList.of(
                                structElement.child(0),
                                new StringLiteral(fields.get(fieldIndex - 1).getName())
                        )
                );
            }
        }
        return structElement;
    }
}
