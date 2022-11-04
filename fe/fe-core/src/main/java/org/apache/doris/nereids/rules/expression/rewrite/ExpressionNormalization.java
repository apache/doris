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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.rules.expression.rewrite.rules.BetweenToCompoundRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.CharacterLiteralTypeCoercion;
import org.apache.doris.nereids.rules.expression.rewrite.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.SimplifyCastRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * normalize expression of plan rule set.
 */
public class ExpressionNormalization extends ExpressionRewrite {

    public static final List<ExpressionRewriteRule> NORMALIZE_REWRITE_RULES = ImmutableList.of(
            NormalizeBinaryPredicatesRule.INSTANCE,
            BetweenToCompoundRule.INSTANCE,
            InPredicateToEqualToRule.INSTANCE,
            SimplifyNotExprRule.INSTANCE,
            CharacterLiteralTypeCoercion.INSTANCE,
            TypeCoercion.INSTANCE,
            FoldConstantRule.INSTANCE,
            SimplifyCastRule.INSTANCE
    );

    public ExpressionNormalization(ConnectContext context) {
        super(new ExpressionRuleExecutor(NORMALIZE_REWRITE_RULES, context));
    }

    public ExpressionNormalization(ConnectContext context, List<ExpressionRewriteRule> rules) {
        super(new ExpressionRuleExecutor(rules, context));
    }
}

