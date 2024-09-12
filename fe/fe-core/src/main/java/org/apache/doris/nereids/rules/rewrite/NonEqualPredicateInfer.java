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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**NonEqualPredicateInfer*/
public class NonEqualPredicateInfer {
    private static class InferenceGraph {
        enum Relation {
            GT,
            GTE,
            EQ,
            UNDEFINED
        }

        // Save and infer the relationship between inputExpressions
        private Relation[][] graph;
        // slots or literal at both ends of the input predicate, and its index corresponds to the one in the graph.
        private List<Expression> inputExpressions;
        // predicates used in derivation
        private List<ComparisonPredicate> canInferInputPredicates = new ArrayList<>();
        // Elements and their indexes in inputExpressions
        private Map<Expression, Integer> inputExprPosition = new HashMap<>();
        // size of inputExpressions
        private int inputExpressionSize;

        /**Constructor*/
        private InferenceGraph(Set<Expression> inputPredicates) {
            Set<Expression> inputExpressionSet = new HashSet<>();
            for (Expression input : inputPredicates) {
                if (input instanceof ComparisonPredicate && !(input instanceof NullSafeEqual)) {
                    ComparisonPredicate comparisonPredicate = (ComparisonPredicate) input;
                    if (comparisonPredicate.left().equals(comparisonPredicate.right())) {
                        continue;
                    }
                    Set<Slot> leftSlots = comparisonPredicate.left().getInputSlots();
                    Set<Slot> rightSlots = comparisonPredicate.right().getInputSlots();
                    if (leftSlots.isEmpty() && rightSlots.isEmpty()) {
                        continue;
                    }
                    if (!isSlotOrLiteral(comparisonPredicate.left()) || !isSlotOrLiteral(comparisonPredicate.right())) {
                        continue;
                    }
                    inputExpressionSet.add(comparisonPredicate.left());
                    inputExpressionSet.add(comparisonPredicate.right());
                    if (comparisonPredicate instanceof LessThan || comparisonPredicate instanceof LessThanEqual) {
                        canInferInputPredicates.add(comparisonPredicate.commute());
                    } else {
                        canInferInputPredicates.add(comparisonPredicate);
                    }
                }
            }
            inputExpressions = new ArrayList<>(inputExpressionSet);
            inputExpressionSize = inputExpressions.size();
            for (int i = 0; i < inputExpressionSize; ++i) {
                inputExprPosition.put(inputExpressions.get(i), i);
            }
            graph = new Relation[inputExpressionSize][inputExpressionSize];
            initGraph(graph);
            // Add edges to the graph.
            for (ComparisonPredicate cp : canInferInputPredicates) {
                if (cp instanceof GreaterThan) {
                    graph[inputExprPosition.get(cp.left())][inputExprPosition.get(cp.right())] = Relation.GT;
                } else if (cp instanceof GreaterThanEqual) {
                    graph[inputExprPosition.get(cp.left())][inputExprPosition.get(cp.right())] = Relation.GTE;
                } else if (cp instanceof EqualTo) {
                    graph[inputExprPosition.get(cp.left())][inputExprPosition.get(cp.right())] = Relation.EQ;
                    graph[inputExprPosition.get(cp.right())][inputExprPosition.get(cp.left())] = Relation.EQ;
                }
            }
        }

        private void initGraph(Relation[][] g) {
            for (int i = 0; i < inputExpressionSize; ++i) {
                for (int j = 0; j < inputExpressionSize; ++j) {
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
            for (int mid = 0; mid < inputExpressionSize; ++mid) {
                for (int left = 0; left < inputExpressionSize; ++left) {
                    for (int right = 0; right < inputExpressionSize; ++right) {
                        connect(graph, left, right, mid);
                    }
                }
            }
        }

        private List<Integer> topoSort() {
            ArrayList<Integer> order = new ArrayList<>();
            order.ensureCapacity(inputExpressionSize);
            ArrayList<Boolean> visited = new ArrayList<>();
            visited.ensureCapacity(inputExpressionSize);
            for (int i = 0; i < inputExpressionSize; ++i) {
                visited.add(false);
            }
            for (int i = 0; i < inputExpressionSize; ++i) {
                dfs(i, visited, order);
            }
            return order;
        }

        private void dfs(int node, List<Boolean> visited, List<Integer> order) {
            if (visited.get(node)) {
                return;
            }
            visited.set(node, true);
            for (int i = 0; i < inputExpressionSize; ++i) {
                if (graph[node][i] == Relation.GT || graph[node][i] == Relation.GTE) {
                    dfs(i, visited, order);
                }
            }
            order.add(node);
        }

        // Determine whether the slots in a predicate come from only one table
        private boolean isTableFilter(int left, int right) {
            Set<String> qualifiers = new HashSet<>();
            for (Slot slot : inputExpressions.get(left).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
            for (Slot slot : inputExpressions.get(right).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
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

        List<Integer> removeExprEqualToConstant(List<Integer> order) {
            // Find expr equal to constant
            Set<Integer> equalWithLiteral = new HashSet<>();
            for (int i = 0; i < inputExpressionSize; ++i) {
                if (!(inputExpressions.get(i) instanceof Literal)) {
                    continue;
                }
                for (int j = 0; j < inputExpressionSize; ++j) {
                    if (graph[i][j] == Relation.EQ && !(inputExpressions.get(j) instanceof Literal)) {
                        equalWithLiteral.add(j);
                    }
                }
            }
            // Remove expr equal to constant
            List<Integer> orderToInfer = new ArrayList<>();
            for (Integer integer : order) {
                if (equalWithLiteral.contains(integer)) {
                    continue;
                }
                orderToInfer.add(integer);
            }
            return orderToInfer;
        }

        private Relation[][] getNonEqualPredicates() {
            List<Integer> order = topoSort();
            List<Integer> orderToInfer = removeExprEqualToConstant(order);
            //Select predicate:
            // 1. Do not select predicates that can be deduced from the intermediate expr
            // 2. If it is an index column or partition column, reserve the predicate
            Relation[][] chosen = new Relation[inputExpressionSize][inputExpressionSize];
            initGraph(chosen);
            for (int i = 1; i < orderToInfer.size(); ++i) {
                for (int j = 0; j < i; ++j) {
                    int left = orderToInfer.get(i);
                    int right = orderToInfer.get(j);
                    if (graph[left][right] == Relation.EQ) {
                        continue;
                    }
                    if (!isTableFilter(left, right)) {
                        continue;
                    }
                    boolean skip = hasIndexOrPartitionColumn(inputExpressions.get(left), inputExpressions.get(right));
                    boolean deducible = false;
                    for (int m = j + 1; !skip && !deducible && m < i; ++m) {
                        int mid = orderToInfer.get(m);
                        if (inputExpressions.get(mid) instanceof Literal) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        } else if (isTableFilter(left, mid) && isTableFilter(right, mid)) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        }
                    }
                    if (!deducible) {
                        chosen[left][right] = graph[left][right];
                    }
                }
            }
            return chosen;
        }

        private Set<ComparisonPredicate> getInferUsedExpressions() {
            return new HashSet<>(canInferInputPredicates);
        }

        private Set<Expression> generatePredicates(Relation[][] chosen) {
            Set<Expression> newPredicates = new HashSet<>();
            for (int i = 0; i < inputExpressionSize; ++i) {
                for (int j = 0; j < inputExpressionSize; ++j) {
                    if (chosen[i][j] == Relation.GT) {
                        newPredicates.add(TypeCoercionUtils.processComparisonPredicate(
                                normalizePredicate(new GreaterThan(
                                inputExpressions.get(i), inputExpressions.get(j)))).withInferred(true));
                    } else if (chosen[i][j] == Relation.GTE) {
                        newPredicates.add(TypeCoercionUtils.processComparisonPredicate(
                                normalizePredicate(new GreaterThanEqual(
                                inputExpressions.get(i), inputExpressions.get(j)))).withInferred(true));
                    }
                }
            }
            return newPredicates;
        }

        private ComparisonPredicate normalizePredicate(ComparisonPredicate expr) {
            return expr.left().isConstant() && !expr.right().isConstant() ? expr.commute() : expr;
        }

        private static boolean isSlotOrLiteral(Expression expr) {
            return expr instanceof SlotReference || expr instanceof Literal;
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

        private void clear(Relation[][] graph, int left, int right) {
            graph[left][right] = Relation.UNDEFINED;
        }

        // graph中新增了一条从hub1到hub2的边，扩展这个图的边
        private void expand_graph(Relation[][] graph, int hub1, int hub2) {
            // 更新所有节点到hub2的路径（使用hub1->hub2）
            for (int left = 0; left < inputExpressionSize; ++left) {
                connect(graph, left, hub2, hub1);
            }
            // 以hub2为中转节点，更新任意两个节点之间的路径
            for (int l = 0; l < inputExpressionSize; ++l) {
                for (int r = 0; r < inputExpressionSize; ++r) {
                    connect(graph, l, r, hub2);
                }
            }
        }

        private Set<Expression> chooseInputPredicates(Relation[][] chosen) {
            boolean[] keep = new boolean[canInferInputPredicates.size()];
            Relation[][] deduced = new Relation[inputExpressionSize][inputExpressionSize];
            for (int i = 0; i < inputExpressionSize; ++i) {
                for (int j = 0; j < inputExpressionSize; ++j) {
                    deduced[i][j] = chosen[i][j];
                }
            }
            for (int i = 0; i < inputExpressionSize; ++i) {
                deduced[i][i] = Relation.EQ;
            }
            // 对deduced进行推导
            deduce(deduced);
            // 进行谓词选择
            for (int i = 0; i < canInferInputPredicates.size(); ++i) {
                Relation type = getType(canInferInputPredicates.get(i));
                int left = inputExprPosition.get(canInferInputPredicates.get(i).left());
                int right = inputExprPosition.get(canInferInputPredicates.get(i).right());
                if (chosen[left][right] == type) {
                    keep[i] = true;
                    clear(chosen, left, right);
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
            for (int i = 0; i < canInferInputPredicates.size(); ++i) {
                if (!keep[i]) {
                    continue;
                }
                chooseInputs.add(normalizePredicate(canInferInputPredicates.get(i)));
            }
            return chooseInputs;
        }
    }

    /**inferUnequalPredicates*/
    public static Set<Expression> inferUnequalPredicates(Set<Expression> inputs) {
        if (inputs.size() < 2) {
            return inputs;
        }
        InferenceGraph inferGraph = new InferenceGraph(inputs);
        inferGraph.deduce(inferGraph.graph);
        InferenceGraph.Relation[][] chosen = inferGraph.getNonEqualPredicates();
        // 保留inputs的谓词顺序，chosen中的input谓词优先放入到输出中。
        // 同时要保留inputs中无法被chosen推导出来的谓词
        // 如果一个输入谓词，没有被chosen，同时又可以被chosen推导出来，那么这个输入谓词不必保留
        Set<Expression> newPredicates = inferGraph.chooseInputPredicates(chosen);
        newPredicates.addAll(inferGraph.generatePredicates(chosen));
        return newPredicates;
    }
}
