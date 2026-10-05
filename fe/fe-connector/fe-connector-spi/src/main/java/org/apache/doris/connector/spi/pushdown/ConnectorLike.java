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

package org.apache.doris.connector.spi.pushdown;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A LIKE/REGEXP predicate: {@code value LIKE pattern}.
 *
 * <p>{@code pattern} is normally a {@link ConnectorLiteral} holding a {@code String}, but the engine does not
 * guarantee it — a non-literal pattern must be dropped, not guessed at (see the package javadoc, Rule 1).</p>
 *
 * <p><b>{@code LIKE} dialect (Doris semantics — translate to these, not to your remote system's defaults):</b></p>
 * <ul>
 *   <li>{@code %} matches any run of characters, including the empty run.</li>
 *   <li>{@code _} matches exactly one character.</li>
 *   <li>Backslash is the escape character, so {@code \%} and {@code \_} are literal {@code %} / {@code _}.
 *       The three-argument {@code LIKE ... ESCAPE '!'} form never reaches this node (it arrives as a
 *       {@code ConnectorFunctionCall} named {@code like} with three arguments — see the package javadoc,
 *       Rule 6), so a connector handling this node may assume the fixed backslash escape.</li>
 * </ul>
 *
 * <p><b>{@code REGEXP} is UNANCHORED</b> (Doris/MySQL semantics): the pattern may match anywhere inside the
 * value. A remote engine whose regex is whole-string anchored — Lucene's {@code regexp} query, for example —
 * is <b>not</b> a valid target for a verbatim hand-off: {@code REGEXP 'bc'} matches {@code 'abcd'} in Doris
 * and matches nothing when anchored. Anchoring narrows the predicate, which Rule 1 forbids; either rewrite
 * the pattern into the remote form or drop the conjunct.</p>
 *
 * <p><b>Do not turn a pattern into a prefix/suffix/contains match unless it is provably equivalent.</b>
 * {@code 'abc%'} is a prefix match. {@code 'a_c%'} is not — {@code _} is a wildcard, so {@code 'abc'} must
 * match the pattern but does not start with {@code a_c}. Neither is {@code 'a\%%'} (that is "starts with
 * {@code a%}"), nor anything with a {@code %} left in the body. A pushed prefix that is stricter than the
 * user's pattern makes the connector skip files, and rows skipped at planning time can never be recovered by
 * BE.</p>
 *
 * <p><b>Case folding: do not introduce it.</b> Neither operator carries a collation here, so translate
 * case-sensitively. A remote form that matches a case-insensitive SUPERSET is permitted by Rule 1 (BE
 * re-checks the original predicate); one that matches fewer rows is not.</p>
 */
public final class ConnectorLike implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    /** LIKE vs REGEXP distinction. */
    public enum Operator {
        LIKE,
        REGEXP
    }

    private final Operator operator;
    private final ConnectorExpression value;
    private final ConnectorExpression pattern;

    public ConnectorLike(Operator operator,
            ConnectorExpression value, ConnectorExpression pattern) {
        this.operator = Objects.requireNonNull(operator, "operator");
        this.value = Objects.requireNonNull(value, "value");
        this.pattern = Objects.requireNonNull(pattern, "pattern");
    }

    public Operator getOperator() {
        return operator;
    }

    public ConnectorExpression getValue() {
        return value;
    }

    public ConnectorExpression getPattern() {
        return pattern;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Arrays.asList(value, pattern);
    }

    @Override
    public String toString() {
        return value + " " + operator + " " + pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorLike)) {
            return false;
        }
        ConnectorLike that = (ConnectorLike) o;
        return operator == that.operator
                && value.equals(that.value) && pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, value, pattern);
    }
}
