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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TableIndexes;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**UnEqualPredicateInfer*/
public class UnequalPredicateInfer {
    private static class InferenceGraph {
        enum Relation {
            GT,
            GTE,
            EQ,
            UNDEFINED
        }

        private static class PairAndRelation {
            private Pair<Expression, Expression> pair;
            private Relation relation;

            private PairAndRelation(Pair<Expression, Expression> p, Relation r) {
                pair = p;
                relation = r;
            }
        }

        // Save and infer the relationship between inputExpressions
        private final Relation[][] graph;
        // slots or literal at both ends of the input predicate, and its index corresponds to the one in the graph.
        private final List<Expression> inputExprs = new ArrayList<>();
        // predicates used in derivation
        private final List<ComparisonPredicate> inputPredicates = new ArrayList<>();
        // Elements and their indexes in inputExpressions
        private final Map<Expression, Integer> inputExprPosition = new HashMap<>();
        // size of inputExprs
        private final int size;
        // not use input predicates
        private final List<Expression> otherPredicates = new ArrayList<>();
        private final List<PairAndRelation> predicatesPairs = new ArrayList<>();

        /**Constructor*/
        private InferenceGraph(Set<ComparisonPredicate> inputs) {
            Set<Expression> inputExpressionSet = new HashSet<>();
            for (ComparisonPredicate comparison : inputs) {
                if (comparison.left().equals(comparison.right())) {
                    otherPredicates.add(comparison);
                    continue;
                }
                Set<Slot> leftSlots = comparison.left().getInputSlots();
                Set<Slot> rightSlots = comparison.right().getInputSlots();
                if (leftSlots.isEmpty() && rightSlots.isEmpty()) {
                    otherPredicates.add(comparison);
                    continue;
                }
                ComparisonPredicate commute;
                if (comparison instanceof LessThan || comparison instanceof LessThanEqual) {
                    commute = (ComparisonPredicate) comparison.commute().withInferred(comparison.isInferred());
                } else if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual
                        || comparison instanceof EqualTo) {
                    commute = comparison;
                } else {
                    otherPredicates.add(comparison);
                    continue;
                }
                Optional<Pair<Expression, Expression>> optionalPair = PredicateInferUtils.getPairFromCast(commute);
                if (!optionalPair.isPresent()) {
                    otherPredicates.add(comparison);
                    continue;
                }
                Pair<Expression, Expression> pair = optionalPair.get();
                if (!PredicateInferUtils.isSlotOrLiteral(pair.first)
                        || !PredicateInferUtils.isSlotOrLiteral(pair.second)) {
                    otherPredicates.add(comparison);
                    continue;
                }
                inputExpressionSet.add(pair.first);
                inputExpressionSet.add(pair.second);
                inputPredicates.add(comparison);
                predicatesPairs.add(new PairAndRelation(pair, getType(commute)));
            }
            inputExprs.addAll(inputExpressionSet);
            size = inputExprs.size();
            for (int i = 0; i < size; ++i) {
                inputExprPosition.put(inputExprs.get(i), i);
            }
            graph = new Relation[size][size];
            initGraph(graph);
            // Add edges to the graph.
            for (PairAndRelation predicatesPair : predicatesPairs) {
                int l = inputExprPosition.get(predicatesPair.pair.first);
                int r = inputExprPosition.get(predicatesPair.pair.second);
                set(graph, l, r, predicatesPair.relation);
            }
        }

        private void initGraph(Relation[][] g) {
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    g[i][j] = Relation.UNDEFINED;
                }
            }
        }

        private void connect(Relation[][] graph, int left, int right, int mid) {
            if (graph[left][right] != Relation.EQ) {
                if (graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.EQ) {
                    graph[left][right] = Relation.EQ;
                }
            }
            if (graph[left][right] != Relation.GTE) {
                if (graph[left][mid] == Relation.GTE && graph[mid][right] == Relation.EQ
                        || graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.GTE) {
                    graph[left][right] = Relation.GTE;
                }
            }
            if (graph[left][right] != Relation.GT) {
                if (graph[left][mid] == Relation.GT && graph[mid][right] != Relation.UNDEFINED
                        || graph[left][mid] != Relation.UNDEFINED && graph[mid][right] == Relation.GT) {
                    graph[left][right] = Relation.GT;
                }
            }
        }

        // Calculate the relationship between left and right derived from mid
        private Relation connectInThisPath(final Relation[][] graph, int left, int right, int mid) {
            Relation deduceRelation = Relation.UNDEFINED;
            if (graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.EQ) {
                deduceRelation = Relation.EQ;
            }
            if (graph[left][mid] == Relation.GTE && graph[mid][right] == Relation.EQ
                    || graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.GTE) {
                deduceRelation = Relation.GTE;
            }
            if (graph[left][mid] == Relation.GT && graph[mid][right] != Relation.UNDEFINED
                    || graph[left][mid] != Relation.UNDEFINED && graph[mid][right] == Relation.GT) {
                deduceRelation = Relation.GT;
            }
            return deduceRelation;
        }

        // use Floyd algorithm to deduce the inequality
        private void deduce(Relation[][] graph) {
            for (int mid = 0; mid < size; ++mid) {
                for (int left = 0; left < size; ++left) {
                    for (int right = 0; right < size; ++right) {
                        connect(graph, left, right, mid);
                    }
                }
            }
        }

        private List<Integer> topoSort() {
            ArrayList<Integer> order = new ArrayList<>();
            order.ensureCapacity(size);
            ArrayList<Boolean> visited = new ArrayList<>();
            visited.ensureCapacity(size);
            for (int i = 0; i < size; ++i) {
                visited.add(false);
            }
            for (int i = 0; i < size; ++i) {
                dfs(i, visited, order);
            }
            return order;
        }

        private void dfs(int node, List<Boolean> visited, List<Integer> order) {
            if (visited.get(node)) {
                return;
            }
            visited.set(node, true);
            for (int i = 0; i < size; ++i) {
                if (graph[node][i] == Relation.GT || graph[node][i] == Relation.GTE) {
                    dfs(i, visited, order);
                }
            }
            order.add(node);
        }

        // Determine whether the slots in a predicate come from only one table
        private boolean isTableFilter(int left, int right) {
            Set<String> qualifiers = new HashSet<>();
            for (Slot slot : inputExprs.get(left).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
            for (Slot slot : inputExprs.get(right).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
            // TODO:
            // isTableFilter(abs(t1.a)#1 = abs(t1.b)#2) will return true
            // isTableFilter(abs(t1.a)#1 = abs(t2.b)#2) will also return true, which is wrong.
            // because expr(e.g. abs(a) #1) qualifiers is empty.
            // We cannot distinguish whether abs(t1.a)#1 = abs(t2.b)#2 is a TableFilter or not.
            // current code may lead to some useful predicates be removed
            return qualifiers.size() == 1;
        }

        private boolean hasIndexOrPartitionColumn(Expression left, Expression right) {
            SlotReference checkSlot;
            if (left instanceof SlotReference && right instanceof Literal) {
                checkSlot = (SlotReference) left;
            } else if (left instanceof Literal && right instanceof SlotReference) {
                checkSlot = (SlotReference) right;
            } else {
                return false;
            }
            if (!checkSlot.isColumnFromTable()) {
                return false;
            }
            Column column = checkSlot.getColumn().get();
            if (column.isKey()) {
                return true;
            }
            if (!checkSlot.getTable().isPresent()) {
                return false;
            }
            TableIf tableIf = checkSlot.getTable().get();
            if (tableIf.isPartitionedTable() && tableIf.isPartitionColumn(column.getName())) {
                return true;
            }

            if (tableIf.getType() != TableType.OLAP) {
                return false;
            }
            TableIndexes tableIndexes = tableIf.getTableIndexes();
            for (Index index : tableIndexes.getIndexes()) {
                IndexDef.IndexType type = index.getIndexType();
                if (type == IndexType.NGRAM_BF || type == IndexType.BLOOMFILTER) {
                    continue;
                }
                Set<String> columns = new HashSet<>(index.getColumns());
                if (columns.contains(column.getName())) {
                    return true;
                }
            }
            return false;
        }

        // determine whether the comparison predicate of type between left right can be deduced by mid
        private boolean checkDeducible(final Relation[][] graph, int left, int right, int mid, Relation type) {
            Relation deduceType = connectInThisPath(graph, left, right, mid);
            return deduceType == type;
        }

        List<Integer> removeExprEqualToConstant(List<Integer> order, Set<Integer> equalWithConstant) {
            // Remove expr equal to constant
            List<Integer> orderToInfer = new ArrayList<>();
            for (Integer integer : order) {
                if (equalWithConstant.contains(integer)) {
                    continue;
                }
                orderToInfer.add(integer);
            }
            return orderToInfer;
        }

        private void getUnequalPredicates(Relation[][] chosen, Set<Integer> equalWithConstant) {
            List<Integer> order = topoSort();
            List<Integer> orderToInfer = removeExprEqualToConstant(order, equalWithConstant);
            //Select predicate:
            // 1. Do not select predicates that can be deduced from the intermediate expr
            // 2. If it is an index column or partition column, reserve the predicate
            for (int i = 1; i < orderToInfer.size(); ++i) {
                for (int j = 0; j < i; ++j) {
                    int left = orderToInfer.get(i);
                    int right = orderToInfer.get(j);
                    if (graph[left][right] == Relation.EQ || graph[left][right] == Relation.UNDEFINED) {
                        continue;
                    }
                    if (!isTableFilter(left, right)) {
                        continue;
                    }
                    boolean skip = hasIndexOrPartitionColumn(inputExprs.get(left), inputExprs.get(right));
                    boolean deducible = false;
                    for (int m = j + 1; !skip && !deducible && m < i; ++m) {
                        int mid = orderToInfer.get(m);
                        if (inputExprs.get(mid) instanceof Literal) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        } else if (isTableFilter(left, mid) && isTableFilter(right, mid)) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        }
                    }
                    if (!deducible) {
                        set(chosen, left, right, graph[left][right]);
                    }
                }
            }
        }

        private Set<Expression> generatePredicates(Relation[][] chosen) {
            Set<Expression> newPredicates = new HashSet<>();
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    if (chosen[i][j] == Relation.GT) {
                        newPredicates.add(TypeCoercionUtils.processComparisonPredicate(
                                normalizePredicate(new GreaterThan(
                                inputExprs.get(i), inputExprs.get(j)))).withInferred(true));
                    } else if (chosen[i][j] == Relation.GTE) {
                        newPredicates.add(TypeCoercionUtils.processComparisonPredicate(
                                normalizePredicate(new GreaterThanEqual(
                                inputExprs.get(i), inputExprs.get(j)))).withInferred(true));
                    }
                }
            }
            return newPredicates;
        }

        private ComparisonPredicate normalizePredicate(ComparisonPredicate expr) {
            return expr.left().isConstant() && !expr.right().isConstant() ? expr.commute() : expr;
        }

        private Relation getType(ComparisonPredicate comparisonPredicate) {
            if (comparisonPredicate instanceof GreaterThan) {
                return Relation.GT;
            } else if (comparisonPredicate instanceof GreaterThanEqual) {
                return Relation.GTE;
            } else if (comparisonPredicate instanceof EqualTo) {
                return Relation.EQ;
            }
            return Relation.UNDEFINED;
        }

        private void clear(Relation[][] graph, int left, int right, Relation type) {
            graph[left][right] = Relation.UNDEFINED;
            if (type == Relation.EQ) {
                graph[left][right] = Relation.UNDEFINED;
            }
        }

        private void set(Relation[][] graph, int left, int right, Relation type) {
            graph[left][right] = type;
            if (type == Relation.EQ) {
                graph[right][left] = type;
            }
        }

        // A new edge from hub1 to hub2 has been added to the graph.
        // Use this edge to extend the connectivity between the graph nodes
        private void expand_graph(Relation[][] graph, int hub1, int hub2) {
            //Update the path from all nodes to hub2 (use hub1->hub2)
            for (int left = 0; left < size; ++left) {
                connect(graph, left, hub2, hub1);
            }
            // Use hub2 as the transit node to update the path between any two nodes
            for (int l = 0; l < size; ++l) {
                for (int r = 0; r < size; ++r) {
                    connect(graph, l, r, hub2);
                }
            }
        }

        private Set<Expression> chooseInputPredicates(Relation[][] chosen) {
            boolean[] keep = new boolean[inputPredicates.size()];
            Relation[][] deduced = new Relation[size][size];
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    deduced[i][j] = chosen[i][j];
                    if (i == j) {
                        deduced[i][j] = Relation.EQ;
                    }
                }
            }
            deduce(deduced);
            // If an input predicate is not chosen and can be deduced by chosen,
            // then the input predicate need not be retained (because it is a useless predicate)
            // And the predicates in inputs that cannot be deduced by chosen should be retained.
            for (int i = 0; i < inputPredicates.size(); ++i) {
                Relation type = predicatesPairs.get(i).relation;
                int left = inputExprPosition.get(predicatesPairs.get(i).pair.first);
                int right = inputExprPosition.get(predicatesPairs.get(i).pair.second);
                if (chosen[left][right] == type) {
                    keep[i] = true;
                    clear(chosen, left, right, type);
                } else if (deduced[left][right] != type) {
                    keep[i] = true;
                    deduced[left][right] = type;
                    expand_graph(deduced, left, right);
                    if (type == Relation.EQ) {
                        expand_graph(deduced, right, left);
                    }
                }
            }
            Set<Expression> chooseInputs = new HashSet<>();
            for (int i = 0; i < inputPredicates.size(); ++i) {
                if (!keep[i]) {
                    continue;
                }
                chooseInputs.add(normalizePredicate(inputPredicates.get(i))
                        .withInferred(inputPredicates.get(i).isInferred()));
            }
            return chooseInputs;
        }

        private Relation[][] chooseEqualPredicates(Set<Integer> equalWithConstant) {
            Relation[][] chosen = new Relation[size][size];
            initGraph(chosen);
            int[] equalToLiteral = new int[size];
            Arrays.fill(equalToLiteral, -1);
            // save equal predicates like a=b (no literal)
            List<Pair<Integer, Integer>> tableFilters = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                for (int j = i + 1; j < size; ++j) {
                    if (graph[i][j] != Relation.EQ) {
                        continue;
                    }
                    // choose predicate with one side literal
                    if (inputExprs.get(i) instanceof Literal && inputExprs.get(j) instanceof Literal) {
                        continue;
                    } else if (inputExprs.get(i) instanceof Literal
                            || inputExprs.get(j) instanceof Literal) {
                        set(chosen, i, j, Relation.EQ);
                        if (inputExprs.get(i) instanceof Literal) {
                            equalToLiteral[j] = i;
                            equalWithConstant.add(j);
                        } else {
                            equalToLiteral[i] = j;
                            equalWithConstant.add(i);
                        }
                    } else if (isTableFilter(i, j)) {
                        tableFilters.add(Pair.of(i, j));
                    }
                }
            }
            // a=b a=c a=1 only infer a=1 b=1 c=1, not retain a=b a=c
            for (Pair<Integer, Integer> tableFilter : tableFilters) {
                int left = tableFilter.first;
                int right = tableFilter.second;
                if (equalToLiteral[left] == -1 || equalToLiteral[right] == -1) {
                    set(chosen, left, right, Relation.EQ);
                    equalToLiteral[left] = left;
                    equalToLiteral[right] = left;
                }
            }
            return chosen;
        }
    }

    /**inferUnequalPredicates*/
    public static Set<? extends Expression> inferUnequalPredicates(Set<ComparisonPredicate> inputs) {
        if (inputs.size() < 2) {
            return inputs;
        }
        InferenceGraph inferGraph = new InferenceGraph(inputs);
        inferGraph.deduce(inferGraph.graph);
        Set<Integer> equalWithConstant = new HashSet<>();
        InferenceGraph.Relation[][] chosen = inferGraph.chooseEqualPredicates(equalWithConstant);
        inferGraph.getUnequalPredicates(chosen, equalWithConstant);
        Set<Expression> newPredicates = inferGraph.chooseInputPredicates(chosen);
        newPredicates.addAll(inferGraph.generatePredicates(chosen));
        newPredicates.addAll(inferGraph.otherPredicates);
        return newPredicates;
    }
}
