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

package org.apache.doris.qe;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;

import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Addresses within one session's mutable point-query plan; never caches parameter values or routes. */
final class PointQueryBindings {
    private final Binding[] conjuncts;
    private final Binding[] keys;
    private final Map<String, Binding> byColumn;
    private StatementContext parameterContext;
    private PlaceholderId[] parameterIds;
    private int[] parameterTypes;
    private Binding[] parameters;
    private Expr[] convertedParameters;

    private PointQueryBindings(Binding[] conjuncts, Binding[] keys, Map<String, Binding> byColumn) {
        this.conjuncts = conjuncts;
        this.keys = keys;
        this.byColumn = byColumn;
    }

    static PointQueryBindings create(List<Column> columns, List<Expr> expressions) {
        if (columns.isEmpty()) {
            return null;
        }
        Map<String, Column> columnsByName = new HashMap<>();
        columns.forEach(column -> columnsByName.put(column.getName(), column));
        Binding[] conjuncts = new Binding[expressions.size()];
        Map<String, Binding> byColumn = new HashMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            Expr expression = expressions.get(i);
            if (!(expression instanceof BinaryPredicate)
                    || ((BinaryPredicate) expression).getOp() != BinaryPredicate.Operator.EQ) {
                return null;
            }
            int literalChild;
            if (expression.getChild(0) instanceof SlotRef && expression.getChild(1) instanceof LiteralExpr) {
                literalChild = 1;
            } else if (expression.getChild(1) instanceof SlotRef && expression.getChild(0) instanceof LiteralExpr) {
                literalChild = 0;
            } else {
                // Keep casts and other expression shapes on the existing path.
                return null;
            }
            SlotRef slot = (SlotRef) expression.getChild(1 - literalChild);
            Column column = columnsByName.get(slot.getColumnName());
            if (column == null) {
                return null;
            }
            Binding binding = new Binding((BinaryPredicate) expression, literalChild, column);
            if (byColumn.put(binding.slot.getColumnName(), binding) != null) {
                return null;
            }
            conjuncts[i] = binding;
        }
        Binding[] keys = new Binding[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            keys[i] = byColumn.get(columns.get(i).getName());
            if (keys[i] == null) {
                return null;
            }
        }
        return new PointQueryBindings(conjuncts, keys, byColumn);
    }

    boolean matches(List<Expr> expressions) {
        if (expressions.size() != conjuncts.length) {
            return false;
        }
        for (int i = 0; i < conjuncts.length; i++) {
            Binding binding = conjuncts[i];
            if (expressions.get(i) != binding.predicate
                    || binding.predicate.getOp() != BinaryPredicate.Operator.EQ
                    || binding.predicate.getChild(1 - binding.literalChild) != binding.slot
                    || !(binding.predicate.getChild(binding.literalChild) instanceof LiteralExpr)) {
                return false;
            }
        }
        return true;
    }

    Expr getKeyLiteral(int index) {
        Binding key = keys[index];
        return key.predicate.getChild(key.literalChild);
    }

    boolean bindParameters(StatementContext context, PrepareCommand command) throws TException {
        List<Placeholder> placeholders = command.getPlaceholders();
        if (parameters == null || parameterContext != context || placeholders.size() != parameters.length) {
            if (!initializeParameters(context, placeholders)) {
                return false;
            }
        }
        for (int i = 0; i < placeholders.size(); i++) {
            Placeholder placeholder = placeholders.get(i);
            if (!parameterIds[i].equals(placeholder.getPlaceholderId())
                    || parameterTypes[i] != placeholder.getMysqlTypeCode()) {
                // Let the existing path handle this execution after a parameter type/ID change.
                parameters = null;
                return false;
            }
            Expression value = context.getIdToPlaceholderRealExpr().get(parameterIds[i]);
            if (!(value instanceof Literal) || value.isNullLiteral()) {
                return false;
            }
        }
        // Convert all values before modifying the cached AST. The scratch array is session-local.
        try {
            for (int i = 0; i < parameters.length; i++) {
                convertedParameters[i] = PointQueryExecutor.getKeyColumnLiteral(
                        ((Literal) context.getIdToPlaceholderRealExpr().get(parameterIds[i])).toLegacyLiteral(),
                        parameters[i].column);
            }
            for (int i = 0; i < parameters.length; i++) {
                parameters[i].predicate.setChild(parameters[i].literalChild, convertedParameters[i]);
            }
        } finally {
            java.util.Arrays.fill(convertedParameters, null);
        }
        return true;
    }

    private boolean initializeParameters(StatementContext context, List<Placeholder> placeholders) {
        if (context.getIdToComparisonSlot().size() != placeholders.size()) {
            return false;
        }
        Binding[] bindings = new Binding[placeholders.size()];
        Set<Binding> seen = new HashSet<>();
        for (int i = 0; i < placeholders.size(); i++) {
            SlotReference slot = context.getIdToComparisonSlot().get(placeholders.get(i).getPlaceholderId());
            if (slot == null || !slot.getOriginalColumn().isPresent()) {
                return false;
            }
            bindings[i] = byColumn.get(slot.getOriginalColumn().get().getName());
            if (bindings[i] == null || !seen.add(bindings[i])) {
                return false;
            }
        }
        parameterIds = new PlaceholderId[bindings.length];
        parameterTypes = new int[bindings.length];
        convertedParameters = new Expr[bindings.length];
        for (int i = 0; i < bindings.length; i++) {
            parameterIds[i] = placeholders.get(i).getPlaceholderId();
            parameterTypes[i] = placeholders.get(i).getMysqlTypeCode();
        }
        parameterContext = context;
        parameters = bindings;
        return true;
    }

    private static final class Binding {
        private final BinaryPredicate predicate;
        private final SlotRef slot;
        private final int literalChild;
        private final Column column;

        private Binding(BinaryPredicate predicate, int literalChild, Column column) {
            this.column = column;
            this.predicate = predicate;
            this.literalChild = literalChild;
            this.slot = (SlotRef) predicate.getChild(1 - literalChild);
        }
    }
}
