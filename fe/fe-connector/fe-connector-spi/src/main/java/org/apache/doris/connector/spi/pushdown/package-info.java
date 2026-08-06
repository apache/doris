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

/**
 * The engine-neutral predicate language the engine hands to a connector, and the rules for translating it.
 *
 * <p>This is the ONLY vocabulary a connector receives for filter pushdown. A connector translates it into its
 * own dialect (an iceberg {@code Expression}, a paimon {@code Predicate}, an ES query DSL document, a SQL
 * {@code WHERE} fragment). Getting a translation subtly wrong does not fail the query — it silently returns
 * the wrong number of rows, and {@code EXPLAIN} shows nothing unusual. So read Rule 1 before writing a
 * converter; the three shipped row-loss bugs fixed in this area were all the same mistake.</p>
 *
 * <p>Rules 1–3 are the contract. Rules 4–6 record what the engine actually produces and what it actually does
 * with your answer, each citing the engine class so it can be re-verified (see Rule 6 of
 * {@code org.apache.doris.connector.spi} — the engine is not on a connector's classpath, so an unverifiable
 * sentence here is worse than no sentence).</p>
 *
 * <h2>Rule 1 — translate exactly, or drop the whole conjunct. Never narrow it</h2>
 *
 * <pre>
 *   can express it exactly    -&gt; push it down
 *   cannot express it exactly -&gt; drop the WHOLE conjunct and let BE evaluate it
 *   never                     -&gt; push a "close enough" predicate that matches FEWER rows
 * </pre>
 *
 * <p>A pushed predicate that is too WIDE is recoverable: the engine keeps the conjuncts and BE re-evaluates
 * them, so the extra rows are filtered out before the user sees them. A pushed predicate that is too NARROW
 * is not recoverable by anything: the connector has already skipped files, partitions or row groups, and the
 * rows never reach BE. That asymmetry — not convenience — is why dropping is always allowed and narrowing
 * never is.</p>
 *
 * <p><b>The precondition on "wide is safe".</b> It holds only while the engine still has the conjuncts. If
 * you tell the engine you consumed them (a {@code null} remaining filter, or index-level tracking — Rule 5),
 * then a predicate that is too wide starts returning EXTRA rows. Read Rule 5 before claiming full pushdown.</p>
 *
 * <p>Two concrete instances of the mistake, both shipped and both fixed:</p>
 * <ul>
 * <li>{@code c <=> 5} translated to {@code c IS NULL}. Null-safe equality against a NON-NULL literal is plain
 *     equality; only against a NULL literal is it {@code IS NULL}. Collapsing the two cases returned zero
 *     rows for a table whose only match was {@code c = 5}. See {@link ConnectorComparison}.</li>
 * <li>{@code s LIKE 'a_c%'} translated to "starts with {@code a_c}". {@code _} is a single-character
 *     wildcard, so {@code 'abc'} matches the user's pattern but not the prefix. See {@link ConnectorLike}.</li>
 * </ul>
 *
 * <h2>Rule 2 — which direction is safe depends on what the predicate is FOR</h2>
 *
 * <p>The same expression tree is used for three purposes, and "drop it" is only safe in two of them:</p>
 *
 * <table border="1">
 * <caption>Safe direction per use</caption>
 * <tr><th>Use</th><th>Dropping a conjunct means</th><th>Correct behavior</th></tr>
 * <tr><td>Scan pushdown ({@link org.apache.doris.connector.spi.ConnectorPushdownOps#applyFilter},
 *         {@code ConnectorScanPlanProvider.planScan})</td>
 *     <td>the filter widens; BE re-evaluates and covers it</td>
 *     <td>dropping allowed, narrowing forbidden</td></tr>
 * <tr><td>Write-time conflict detection
 *         ({@link org.apache.doris.connector.spi.handle.ConnectorTransaction#applyWriteConstraint})</td>
 *     <td>conflict detection widens, i.e. gets more conservative</td>
 *     <td>dropping allowed</td></tr>
 * <tr><td>{@code ALTER TABLE ... EXECUTE ... WHERE} rewrite scoping</td>
 *     <td>MORE files get rewritten — dropping the whole WHERE rewrites the entire table</td>
 *     <td><b>must throw</b>; dropping is not allowed</td></tr>
 * </table>
 *
 * <p>The engine already enforces the third row on its side: {@code UnboundExpressionToConnectorPredicateConverter}
 * is strictly all-or-nothing and throws when any part of the {@code WHERE} cannot be represented neutrally.
 * A connector consuming such a predicate must hold the symmetric invariant — a conjunct it cannot turn into
 * file pruning is a hard error there, not a silent drop.</p>
 *
 * <h2>Rule 3 — column names, and what is NOT specified here</h2>
 *
 * <p>{@link ConnectorColumnRef#getColumnName()} carries the name as the engine knows it. Case handling is
 * <b>currently decided per connector</b> and is deliberately not specified: paimon lower-cases both sides
 * before matching field names, jdbc maps through its own remote-name table. If your remote system is
 * case-sensitive, decide explicitly rather than assuming the engine normalized anything.</p>
 *
 * <h2>Rule 4 — what the engine actually produces</h2>
 *
 * <p><b>One producer.</b> Every tree is built by {@code ExprToConnectorExpressionConverter} (fe-core). The
 * other two entry points — {@code NereidsToConnectorExpressionConverter} for DELETE/UPDATE/MERGE and
 * {@code UnboundExpressionToConnectorPredicateConverter} for {@code EXECUTE ... WHERE} — route literals and
 * types back through it, so literal encoding is identical on all three paths.</p>
 *
 * <p><b>Shape of the root.</b> {@code convertConjuncts} returns the single node directly when there is
 * exactly one conjunct (<b>the root is NOT a {@link ConnectorAnd}</b>), and a boolean {@code true}
 * {@link ConnectorLiteral} when there are none. Do not assume an {@code AND} root.</p>
 *
 * <p><b>Two arrival paths, only one of which strips CAST.</b></p>
 * <ul>
 * <li>{@link ConnectorFilterConstraint} handed to
 *     {@link org.apache.doris.connector.spi.ConnectorPushdownOps#applyFilter} is built by
 *     {@code PluginDrivenScanNode.buildFilterConstraint} and is <b>NOT</b> filtered:
 *     {@code supportsCastPredicatePushdown} is not consulted, so predicates wrapping a CAST reach you as-is
 *     (the converter unwraps the {@code CastExpr} node itself, handing you the inner expression).</li>
 * <li>The {@code Optional<ConnectorExpression> filter} handed to {@code ConnectorScanPlanProvider.planScan}
 *     and {@code getScanNodePropertiesResult} comes from {@code PluginDrivenScanNode.buildRemainingFilter},
 *     which DOES drop conjuncts containing a CAST when
 *     {@link org.apache.doris.connector.spi.ConnectorPushdownOps#supportsCastPredicatePushdown} is false.</li>
 * </ul>
 *
 * <p><b>Literal value domain.</b> {@link ConnectorLiteral#getValue()} is one of exactly eight Java shapes.
 * Note that {@code Integer} is never produced and that {@code LARGEINT} arrives as a decimal
 * {@code String}:</p>
 * <table border="1">
 * <caption>Doris literal type to Java value</caption>
 * <tr><th>Doris type</th><th>Java class</th></tr>
 * <tr><td>NULL literal (any type)</td><td>{@code null} — see {@link ConnectorLiteral#isNull()}</td></tr>
 * <tr><td>BOOLEAN</td><td>{@code Boolean}</td></tr>
 * <tr><td>TINYINT / SMALLINT / INT / BIGINT</td><td>{@code Long} (never {@code Integer})</td></tr>
 * <tr><td>FLOAT / DOUBLE</td><td>{@code Double}</td></tr>
 * <tr><td>DECIMAL*</td><td>{@code BigDecimal}</td></tr>
 * <tr><td>CHAR / VARCHAR / STRING</td><td>{@code String}</td></tr>
 * <tr><td>DATE / DATEV2</td><td>{@code LocalDate}</td></tr>
 * <tr><td>DATETIME / DATETIMEV2</td><td>{@code LocalDateTime}</td></tr>
 * <tr><td>anything else, incl. LARGEINT, IPV4/IPV6, JSON</td>
 *     <td>{@code String} from {@code Expr.getStringValue()}</td></tr>
 * </table>
 *
 * <h2>Rule 5 — telling the engine what you consumed: two protocols, one of which does nothing</h2>
 *
 * <table border="1">
 * <caption>Residual protocols and their real effect</caption>
 * <tr><th>What you return</th><th>What the engine does</th></tr>
 * <tr><td>{@link FilterApplicationResult#getRemainingFilter()} == {@code null}</td>
 *     <td>{@code PluginDrivenScanNode.convertPredicate} clears ALL conjuncts. BE re-evaluates nothing, so
 *         every predicate you were given must have been pushed EXACTLY.</td></tr>
 * <tr><td>{@code getRemainingFilter()} != {@code null} (any expression, including the original one)</td>
 *     <td><b>Nothing is removed.</b> The engine keeps every conjunct; matching residual sub-expressions back
 *         to individual conjuncts is not implemented. Returning "the half I did not push" does not give you
 *         credit for the half you did.</td></tr>
 * <tr><td>{@code ScanNodePropertiesResult} with not-pushed conjunct indices</td>
 *     <td>{@code PluginDrivenScanNode.pruneConjunctsFromNodeProperties} really does prune: conjuncts whose
 *         index you did NOT report are removed. This is the only protocol with per-conjunct granularity;
 *         it is used by exactly one shipped connector (es).</td></tr>
 * </table>
 *
 * <p>Consequence for everyone else: all three {@code applyFilter} implementations shipped today return the
 * original expression as the residual, so their predicates are also evaluated on BE. That is correct but not
 * free — and it is the reason a slightly-too-wide pushdown is survivable today.</p>
 *
 * <h2>Rule 6 — {@link ConnectorFunctionCall} is also the fallback carrier. Do not match it by name</h2>
 *
 * <p>{@code ExprToConnectorExpressionConverter.fallback} turns any expression it does not model into a
 * {@link ConnectorFunctionCall} whose <b>name is the rendered Doris SQL text</b> and whose argument list is
 * <b>empty</b>. It is not a function call at all. Matching function names blindly will make a connector
 * translate {@code my_udf(a, b) = 1} as a call to a function literally named {@code "my_udf(a, b) = 1"}.
 * jdbc handles this deliberately: an argument-less call whose name is not a plain identifier is emitted as a
 * pre-rendered SQL fragment ({@code JdbcQueryBuilder}).</p>
 *
 * <p>Two related shapes to expect:</p>
 * <ul>
 * <li>{@code LIKE ... ESCAPE '!'} (the three-argument form) does <b>not</b> arrive as a
 *     {@link ConnectorLike} — the converter only builds one for the two-argument form, so it arrives as a
 *     {@code ConnectorFunctionCall} named {@code like} with three arguments. A connector that only handles
 *     {@link ConnectorLike} therefore never sees a custom escape character, and may assume the fixed
 *     backslash escape documented on {@link ConnectorLike}.</li>
 * <li>Arithmetic and genuine scalar functions arrive as {@link ConnectorFunctionCall} with real arguments;
 *     those are safe to match by name.</li>
 * </ul>
 *
 * <p>Anything you cannot interpret confidently falls under Rule 1: drop the conjunct.</p>
 */
package org.apache.doris.connector.spi.pushdown;
