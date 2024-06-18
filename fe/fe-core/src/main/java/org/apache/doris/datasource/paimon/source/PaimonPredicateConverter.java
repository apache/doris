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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class PaimonPredicateConverter {
    private final PredicateBuilder builder;
    private final List<String> fieldNames;
    private final List<DataType> paimonFieldTypes;

    public PaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.fieldNames = rowType.getFields().stream().map(f -> f.name().toLowerCase()).collect(Collectors.toList());
        this.paimonFieldTypes = rowType.getFields().stream().map(DataField::type).collect(Collectors.toList());
    }

    public List<Predicate> convertToPaimonExpr(List<Expr> conjuncts) {
        List<Predicate> list = new ArrayList<>(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            Predicate predicate = convertToPaimonExpr(conjunct);
            if (predicate != null) {
                list.add(predicate);
            }
        }
        return list;
    }

    private Predicate convertToPaimonExpr(Expr dorisExpr) {
        if (dorisExpr == null) {
            return null;
        }
        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            Predicate left = convertToPaimonExpr(compoundPredicate.getChild(0));
            Predicate right = convertToPaimonExpr(compoundPredicate.getChild(1));

            switch (compoundPredicate.getOp()) {
                case AND: {
                    if (left != null && right != null) {
                        return PredicateBuilder.and(left, right);
                    }
                    return null;
                }
                case OR: {
                    if (left != null && right != null) {
                        return PredicateBuilder.or(left, right);
                    }
                    return null;
                }
                default:
                    return null;
            }
        } else {
            return binaryExprDesc(dorisExpr);
        }
    }

    private Predicate binaryExprDesc(Expr dorisExpr) {
        TExprOpcode opcode = dorisExpr.getOpcode();
        // Make sure the col slot is always first
        SlotRef slotRef = convertDorisExprToSlotRef(dorisExpr.getChild(0));
        LiteralExpr literalExpr = convertDorisExprToLiteralExpr(dorisExpr.getChild(1));
        if (slotRef == null || literalExpr == null) {
            return null;
        }
        String colName = slotRef.getColumnName();
        int idx = fieldNames.indexOf(colName);
        DataType dataType = paimonFieldTypes.get(idx);
        Object value = dataType.accept(new PaimonValueConverter(literalExpr));
        if (value == null) {
            return null;
        }
        switch (opcode) {
            case EQ:
                return builder.equal(idx, value);
            case EQ_FOR_NULL:
                return builder.isNull(idx);
            case NE:
                return builder.notEqual(idx, value);
            case GE:
                return builder.greaterOrEqual(idx, value);
            case GT:
                return builder.greaterThan(idx, value);
            case LE:
                return builder.lessOrEqual(idx, value);
            case LT:
                return builder.lessThan(idx, value);
            case INVALID_OPCODE:
                if (dorisExpr instanceof FunctionCallExpr) {
                    String name = dorisExpr.getExprName().toLowerCase();
                    String s = value.toString();
                    if (name.equals("like") && !s.startsWith("%") && s.endsWith("%")) {
                        return builder.startsWith(idx, BinaryString.fromString(s.substring(0, s.length() - 1)));
                    }
                } else if (dorisExpr instanceof IsNullPredicate) {
                    if (((IsNullPredicate) dorisExpr).isNotNull()) {
                        return builder.isNotNull(idx);
                    } else {
                        return builder.isNull(idx);
                    }
                }
                return null;
            default:
                return null;
        }

    }


    public static SlotRef convertDorisExprToSlotRef(Expr expr) {
        SlotRef slotRef = null;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                slotRef = (SlotRef) expr.getChild(0);
            }
        }
        return slotRef;
    }

    public LiteralExpr convertDorisExprToLiteralExpr(Expr expr) {
        LiteralExpr literalExpr = null;
        if (expr instanceof LiteralExpr) {
            literalExpr = (LiteralExpr) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof LiteralExpr) {
                literalExpr = (LiteralExpr) expr.getChild(0);
            }
        }
        return literalExpr;
    }
}
