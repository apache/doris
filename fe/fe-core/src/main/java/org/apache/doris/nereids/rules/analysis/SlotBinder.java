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

import org.apache.doris.analysis.SetType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.VariableVarConverters;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class SlotBinder extends SubExprAnalyzer {
    /*
    bounded={table.a, a}
    unbound=a
    if enableExactMatch, 'a' is bound to bounded 'a',
    if not enableExactMatch, 'a' is ambiguous
    in order to be compatible to original planner,
    exact match mode is not enabled for having clause
    but enabled for order by clause
    TODO after remove original planner, always enable exact match mode.
     */
    private final boolean enableExactMatch;
    private final boolean bindSlotInOuterScope;

    public SlotBinder(Scope scope, CascadesContext cascadesContext) {
        this(scope, cascadesContext, true, true);
    }

    public SlotBinder(Scope scope, CascadesContext cascadesContext,
            boolean enableExactMatch, boolean bindSlotInOuterScope) {
        super(scope, cascadesContext);
        this.enableExactMatch = enableExactMatch;
        this.bindSlotInOuterScope = bindSlotInOuterScope;
    }

    public Expression bind(Expression expression) {
        return expression.accept(this, null);
    }

    @Override
    public Expression visitUnboundVariable(UnboundVariable unboundVariable, CascadesContext context) {
        String name = unboundVariable.getName();
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        Literal literal = null;
        if (unboundVariable.getType() == VariableType.DEFAULT) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.DEFAULT);
        } else if (unboundVariable.getType() == VariableType.SESSION) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.SESSION);
        } else if (unboundVariable.getType() == VariableType.GLOBAL) {
            literal = VariableMgr.getLiteral(sessionVariable, name, SetType.GLOBAL);
        } else if (unboundVariable.getType() == VariableType.USER) {
            literal = VariableMgr.getLiteralForUserVar(name);
        }
        if (literal == null) {
            throw new AnalysisException("Unsupported system variable: " + unboundVariable.getName());
        }
        if (!Strings.isNullOrEmpty(name) && VariableVarConverters.hasConverter(name)) {
            try {
                Preconditions.checkArgument(literal instanceof IntegerLikeLiteral);
                IntegerLikeLiteral integerLikeLiteral = (IntegerLikeLiteral) literal;
                literal = new StringLiteral(VariableVarConverters.decode(name, integerLikeLiteral.getLongValue()));
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
        return new Variable(unboundVariable.getName(), unboundVariable.getType(), literal);
    }

    @Override
    public Expression visitUnboundAlias(UnboundAlias unboundAlias, CascadesContext context) {
        Expression child = unboundAlias.child().accept(this, context);
        if (unboundAlias.getAlias().isPresent()) {
            return new Alias(child, unboundAlias.getAlias().get());
        } else if (child instanceof NamedExpression) {
            return new Alias(child, ((NamedExpression) child).getName());
        } else {
            return new Alias(child);
        }
    }

    @Override
    public Slot visitUnboundSlot(UnboundSlot unboundSlot, CascadesContext context) {
        Optional<List<Slot>> boundedOpt = Optional.of(bindSlot(unboundSlot, getScope().getSlots()));
        boolean foundInThisScope = !boundedOpt.get().isEmpty();
        // Currently only looking for symbols on the previous level.
        if (bindSlotInOuterScope && !foundInThisScope && getScope().getOuterScope().isPresent()) {
            boundedOpt = Optional.of(bindSlot(unboundSlot,
                    getScope()
                            .getOuterScope()
                            .get()
                            .getSlots()));
        }
        List<Slot> bounded = boundedOpt.get();
        switch (bounded.size()) {
            case 0:
                // just return, give a chance to bind on another slot.
                // if unbound finally, check will throw exception
                return unboundSlot;
            case 1:
                if (!foundInThisScope
                        && !getScope().getOuterScope().get().getCorrelatedSlots().contains(bounded.get(0))) {
                    getScope().getOuterScope().get().getCorrelatedSlots().add(bounded.get(0));
                }
                return bounded.get(0);
            default:
                if (enableExactMatch) {
                    // select t1.k k, t2.k
                    // from t1 join t2 order by k
                    //
                    // 't1.k k' is denoted by alias_k, its full name is 'k'
                    // 'order by k' is denoted as order_k, it full name is 'k'
                    // 't2.k' in select list, its full name is 't2.k'
                    //
                    // order_k can be bound on alias_k and t2.k
                    // alias_k is exactly matched, since its full name is exactly match full name of order_k
                    // t2.k is not exactly matched, since t2.k's full name is larger than order_k
                    List<Slot> exactMatch = bounded.stream()
                            .filter(bound -> unboundSlot.getNameParts().size() == bound.getQualifier().size() + 1)
                            .collect(Collectors.toList());
                    if (exactMatch.size() == 1) {
                        return exactMatch.get(0);
                    }
                }
                throw new AnalysisException(String.format("%s is ambiguous: %s.",
                        unboundSlot.toSql(),
                        bounded.stream()
                                .map(Slot::toString)
                                .collect(Collectors.joining(", "))));
        }
    }

    @Override
    public Expression visitUnboundStar(UnboundStar unboundStar, CascadesContext context) {
        List<String> qualifier = unboundStar.getQualifier();
        boolean showHidden = Util.showHiddenColumns();
        List<Slot> slots = getScope().getSlots()
                .stream()
                .filter(slot -> !(slot instanceof SlotReference)
                || (((SlotReference) slot).isVisible()) || showHidden)
                .collect(Collectors.toList());
        switch (qualifier.size()) {
            case 0: // select *
                return new BoundStar(slots);
            case 1: // select table.*
            case 2: // select db.table.*
            case 3: // select catalog.db.table.*
                return bindQualifiedStar(qualifier, slots);
            default:
                throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
        }
    }

    private BoundStar bindQualifiedStar(List<String> qualifierStar, List<Slot> boundSlots) {
        // FIXME: compatible with previous behavior:
        // https://github.com/apache/doris/pull/10415/files/3fe9cb0c3f805ab3a9678033b281b16ad93ec60a#r910239452
        List<Slot> slots = boundSlots.stream().filter(boundSlot -> {
            switch (qualifierStar.size()) {
                // table.*
                case 1:
                    List<String> boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                            return false;
                        case 1: // bound slot is `table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0));
                        case 2:// bound slot is `db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(1));
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, "."));
                    }
                case 2: // db.table.*
                    boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                        case 1: // bound slot is `table`.`column`
                            return false;
                        case 2:// bound slot is `db`.`table`.`column`
                            return compareDbNameIgnoreClusterName(qualifierStar.get(0), boundSlotQualifier.get(0))
                                    && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(1));
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return compareDbNameIgnoreClusterName(qualifierStar.get(0), boundSlotQualifier.get(1))
                                && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                    }
                case 3: // catalog.db.table.*
                    boundSlotQualifier = boundSlot.getQualifier();
                    switch (boundSlotQualifier.size()) {
                        // bound slot is `column` and no qualified
                        case 0:
                        case 1: // bound slot is `table`.`column`
                        case 2: // bound slot is `db`.`table`.`column`
                            return false;
                        case 3:// bound slot is `catalog`.`db`.`table`.`column`
                            return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0))
                                && compareDbNameIgnoreClusterName(qualifierStar.get(1), boundSlotQualifier.get(1))
                                && qualifierStar.get(2).equalsIgnoreCase(boundSlotQualifier.get(2));
                        default:
                            throw new AnalysisException("Not supported qualifier: "
                                + StringUtils.join(qualifierStar, ".") + ".*");
                    }
                default:
                    throw new AnalysisException("Not supported name: "
                            + StringUtils.join(qualifierStar, ".") + ".*");
            }
        }).collect(Collectors.toList());

        return new BoundStar(slots);
    }

    private boolean compareDbNameIgnoreClusterName(String unBoundDbName, String boundedDbName) {
        if (unBoundDbName.equalsIgnoreCase(boundedDbName)) {
            return true;
        }
        // boundedDbName example
        int idx = boundedDbName.indexOf(ClusterNamespace.CLUSTER_DELIMITER);
        if (idx > -1) {
            return unBoundDbName.equalsIgnoreCase(boundedDbName.substring(idx + 1));
        }
        return false;
    }

    private List<Slot> bindSlot(UnboundSlot unboundSlot, List<Slot> boundSlots) {
        return boundSlots.stream().distinct().filter(boundSlot -> {
            List<String> nameParts = unboundSlot.getNameParts();
            int qualifierSize = boundSlot.getQualifier().size();
            int namePartsSize = nameParts.size();
            if (namePartsSize > qualifierSize + 1) {
                return false;
            }
            if (namePartsSize == 1) {
                return nameParts.get(0).equalsIgnoreCase(boundSlot.getName());
            }
            if (namePartsSize == 2) {
                String qualifierTableName = boundSlot.getQualifier().get(qualifierSize - 1);
                return sameTableName(qualifierTableName, nameParts.get(0))
                        && boundSlot.getName().equalsIgnoreCase(nameParts.get(1));
            }
            if (nameParts.size() == 3) {
                String qualifierTableName = boundSlot.getQualifier().get(qualifierSize - 1);
                String qualifierDbName = boundSlot.getQualifier().get(qualifierSize - 2);
                return compareDbNameIgnoreClusterName(nameParts.get(0), qualifierDbName)
                        && sameTableName(qualifierTableName, nameParts.get(1))
                        && boundSlot.getName().equalsIgnoreCase(nameParts.get(2));
            }
            // catalog.db.table.column
            if (nameParts.size() == 4) {
                String qualifierTableName = boundSlot.getQualifier().get(qualifierSize - 1);
                String qualifierDbName = boundSlot.getQualifier().get(qualifierSize - 2);
                String qualifierCatalogName = boundSlot.getQualifier().get(qualifierSize - 3);
                return qualifierCatalogName.equalsIgnoreCase(nameParts.get(0))
                    && compareDbNameIgnoreClusterName(nameParts.get(1), qualifierDbName)
                    && sameTableName(qualifierTableName, nameParts.get(2))
                    && boundSlot.getName().equalsIgnoreCase(nameParts.get(3));
            }
            //TODO: handle name parts more than three.
            throw new AnalysisException("Not supported name: "
                    + StringUtils.join(nameParts, "."));
        }).collect(Collectors.toList());
    }

    private boolean sameTableName(String boundSlot, String unboundSlot) {
        if (Config.lower_case_table_names != 1) {
            return boundSlot.equals(unboundSlot);
        } else {
            return boundSlot.equalsIgnoreCase(unboundSlot);
        }
    }
}
