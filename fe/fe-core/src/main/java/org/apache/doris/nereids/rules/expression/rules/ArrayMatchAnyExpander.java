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

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMatchAny;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ArrayMatchAnyExpander
 *
 *  expand array_match_any(col1, "a,b") to array_match_any(col1, "a,b", invertedIdxCtx)
 *  if literal array , we should not expand just to go with no inverted index match
 *   like array_match_any(["a", "b", "c", 'd'], "a,b") to array_match_any(col1, "a,b")
 *
 * after expand, we can evaluate it.
 */
public class ArrayMatchAnyExpander extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan())
                .when(filter -> ExpressionUtils.containsType(filter.getExpressions(), ArrayMatchAny.class))
                .then(this::replace)
                .toRule(RuleType.ARRAY_MATCH_ANY_EXPANDER);
    }

    Plan replace(LogicalFilter<LogicalOlapScan> filter) {
        Set<Expression> sets = filter.getConjuncts().stream().map(expr -> {
            if (expr instanceof ArrayMatchAny) {
                return rewrite((ArrayMatchAny) expr);
            } else {
                return expr;
            }
        }).collect(ImmutableSet.toImmutableSet());
        return filter.withConjuncts(sets);
    }

    private static Expression rewrite(ArrayMatchAny arrayMatchAny) {
        if (arrayMatchAny.children().size() > 2) {
            return arrayMatchAny;
        }
        // default inverted index ctx
        String invertedIndexParser = InvertedIndexUtil.INVERTED_INDEX_PARSER_ENGLISH;
        String invertedIndexParserMode = InvertedIndexUtil.INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
        List<Literal> keys = Lists.newArrayList(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY,
                        InvertedIndexUtil.INVERTED_INDEX_PARSER_MODE_KEY).stream()
                        .map(StringLiteral::new).collect(Collectors.toList());
        if (arrayMatchAny.child(0) instanceof SlotReference
                && ((SlotReference) arrayMatchAny.child(0)).getTable().isPresent()
                && ((SlotReference) arrayMatchAny.child(0)).getTable().get() instanceof OlapTable) {
            // here should a table in column array, we should extract the inverted index context
            SlotReference left = (SlotReference) arrayMatchAny.child(0);
            OlapTable olapTbl = (OlapTable) ((SlotReference) arrayMatchAny.child(0)).getTable().get();
            List<Index> indexes = olapTbl.getIndexes();
            if (indexes != null) {
                Map<String, String> invertedIndexCharFilter = new HashMap<>();
                for (Index index : indexes) {
                    if (index.getIndexType() == IndexDef.IndexType.INVERTED) {
                        List<String> columns = index.getColumns();
                        if (columns != null && !columns.isEmpty() && left.getName().equals(columns.get(0))) {
                            invertedIndexParser = index.getInvertedIndexParser();
                            invertedIndexParserMode = index.getInvertedIndexParserMode();
                            invertedIndexCharFilter = index.getInvertedIndexCharFilter();
                            break;
                        }
                    }
                }
                List<Literal> values = Lists.newArrayList(invertedIndexParser, invertedIndexParserMode).stream()
                                .map(StringLiteral::new).collect(Collectors.toList());
                invertedIndexCharFilter.forEach((key, value) -> {
                    keys.add(new StringLiteral(key));
                    values.add(new StringLiteral(value));
                });

                MapLiteral invertedIndexParserMap = new MapLiteral(keys, values);
                return new ArrayMatchAny(arrayMatchAny.child(0), arrayMatchAny.child(1), invertedIndexParserMap);
            } else {
                List<Literal> values = Lists.newArrayList(invertedIndexParser, invertedIndexParserMode).stream()
                                .map(StringLiteral::new).collect(Collectors.toList());
                MapLiteral invertedIndexParserMap = new MapLiteral(keys, values);
                return new ArrayMatchAny(arrayMatchAny.child(0), arrayMatchAny.child(1), invertedIndexParserMap);
            }
        } else {
            List<Literal> values = Lists.newArrayList(invertedIndexParser, invertedIndexParserMode).stream()
                            .map(StringLiteral::new).collect(Collectors.toList());
            MapLiteral invertedIndexParserMap = new MapLiteral(keys, values);
            return new ArrayMatchAny(arrayMatchAny.child(0), arrayMatchAny.child(1), invertedIndexParserMap);
        }
    }
}
