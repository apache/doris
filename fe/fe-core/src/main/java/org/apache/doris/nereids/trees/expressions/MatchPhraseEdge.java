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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * like expression: a MATCH_PHRASE_EDGE 'llo w'.
 */
public class MatchPhraseEdge extends Match {
    public MatchPhraseEdge(Expression left, Expression right) {
        super(ImmutableList.of(left, right), "MATCH_PHRASE_EDGE");
    }

    private MatchPhraseEdge(List<Expression> children) {
        super(children, "MATCH_PHRASE_EDGE");
    }

    @Override
    public MatchPhraseEdge withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MatchPhraseEdge(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMatchPhraseEdge(this, context);
    }
}
