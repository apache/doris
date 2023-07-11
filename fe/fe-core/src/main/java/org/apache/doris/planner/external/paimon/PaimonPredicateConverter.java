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

package org.apache.doris.planner.external.paimon;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.thrift.TExprOpcode;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


public class PaimonPredicateConverter {
    private final PredicateBuilder builder;
    private final List<String> fieldNames;

    public PaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.fieldNames = rowType.getFields().stream().map(DataField::name).collect(Collectors.toList());
    }

    public List<Predicate> convertToPaimonExpr(List<Expr> conjuncts) {
        List<Predicate> list = new ArrayList<>(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            Predicate predicate = convertToPaimonExpr(conjunct);
            list.add(predicate);
        }
        return list;
    }

    public Predicate convertToPaimonExpr(Expr dorisExpr) {
        if (dorisExpr == null) {
            return null;
        }
        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            Predicate left = convertToPaimonExpr(compoundPredicate.getChild(0));
            Predicate right = convertToPaimonExpr(compoundPredicate.getChild(1));

            switch (compoundPredicate.getOp()) {
                case AND: {
                    return PredicateBuilder.and(left, right);
                }
                case OR: {
                    return PredicateBuilder.or(left, right);
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
        switch (opcode) {
            case EQ:
            case NE:
            case GE:
            case GT:
            case LE:
            case LT:
            case EQ_FOR_NULL:
                BinaryPredicate eq = (BinaryPredicate) dorisExpr;
                // Make sure the col slot is always first
                SlotRef slotRef = convertDorisExprToSlotRef(eq.getChild(0));
                LiteralExpr literalExpr = convertDorisExprToLiteralExpr(eq.getChild(1));
                if (slotRef == null || literalExpr == null) {
                    return null;
                }
                String colName = slotRef.getColumnName();
                int idx = fieldNames.indexOf(colName);
                Object value = extractDorisLiteral(literalExpr);
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
                    default:
                        return null;
                }
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

    public Object extractDorisLiteral(Expr expr) {
        if (!expr.isLiteral()) {
            return null;
        }
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(ZoneId.systemDefault());
            StringBuilder sb = new StringBuilder();
            sb.append(dateLiteral.getYear())
                    .append(dateLiteral.getMonth())
                    .append(dateLiteral.getDay())
                    .append(dateLiteral.getHour())
                    .append(dateLiteral.getMinute())
                    .append(dateLiteral.getSecond());
            Date date;
            try {
                date = Date.from(
                        LocalDateTime.parse(sb.toString(), formatter).atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException e) {
                return null;
            }
            return Timestamp.fromEpochMillis(date.getTime());
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            BigDecimal value = decimalLiteral.getValue();
            return Decimal.fromBigDecimal(value, value.precision(), value.scale());
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        } else if (expr instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) expr;
            return BinaryString.fromString(stringLiteral.getStringValue());
        }
        return null;
    }
}
