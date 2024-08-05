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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.rules.ReplaceVariableByLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * replace Variable To Literal
 */
public class VariableToLiteral extends ExpressionRewrite {
    public static final List<ExpressionRewriteRule> NORMALIZE_REWRITE_RULES =
            ImmutableList.of(bottomUp(ReplaceVariableByLiteral.INSTANCE));

    public VariableToLiteral() {
        super(new ExpressionRuleExecutor(NORMALIZE_REWRITE_RULES));
    }
}
