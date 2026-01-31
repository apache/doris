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

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * like expression: a MATCH_PHRASE_EDGE 'llo w'.
 */
public class MatchPhraseEdge extends Match {
    public MatchPhraseEdge(Expression left, Expression right) {
        this(left, right, null);
    }

    public MatchPhraseEdge(Expression left, Expression right, String analyzer) {
        super(ImmutableList.of(left, right), "MATCH_PHRASE_EDGE", analyzer);
    }

    private MatchPhraseEdge(List<Expression> children, String analyzer) {
        super(children, "MATCH_PHRASE_EDGE", analyzer);
    }

    @Override
    protected MatchPhraseEdge createInstance(List<Expression> children, String analyzer) {
        return new MatchPhraseEdge(children, analyzer);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMatchPhraseEdge(this, context);
    }
}
