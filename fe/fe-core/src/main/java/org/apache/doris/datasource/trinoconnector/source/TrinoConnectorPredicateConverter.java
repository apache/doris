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

package org.apache.doris.datasource.trinoconnector.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;


public class TrinoConnectorPredicateConverter {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorPredicateConverter.class);
    private static final String EPOCH_DATE = "1970-01-01";
    private static final String GMT = "GMT";
    private final Map<String, ColumnHandle> trinoConnectorColumnHandleMap;

    private final Map<String, ColumnMetadata> trinoConnectorColumnMetadataMap;

    public TrinoConnectorPredicateConverter(Map<String, ColumnHandle> columnHandleMap,
            Map<String, ColumnMetadata> columnMetadataMap) {
        this.trinoConnectorColumnHandleMap = columnHandleMap;
        this.trinoConnectorColumnMetadataMap = columnMetadataMap;
    }

    public TupleDomain<ColumnHandle> convertExprToTrinoTupleDomain(Expr predicate) throws AnalysisException {
        if (predicate instanceof CompoundPredicate) {
            return compoundPredicateConverter((CompoundPredicate) predicate);
        } else if (predicate instanceof InPredicate) {
            return inPredicateConverter((InPredicate) predicate);
        } else if (predicate instanceof BinaryPredicate) {
            return binaryPredicateConverter((BinaryPredicate) predicate);
        } else if (predicate instanceof IsNullPredicate) {
            return isNullPredicateConverter((IsNullPredicate) predicate);
        } else {
            throw new AnalysisException("Do not support convert predicate: [" + predicate + "].");
        }
    }

    private TupleDomain<ColumnHandle> compoundPredicateConverter(CompoundPredicate compoundPredicate)
            throws AnalysisException {
        switch (compoundPredicate.getOp()) {
            case AND: {
                TupleDomain<ColumnHandle> left = null;
                TupleDomain<ColumnHandle> right = null;
                try {
                    left = convertExprToTrinoTupleDomain(compoundPredicate.getChild(0));
                } catch (AnalysisException e) {
                    LOG.warn("left predicate of compund predicate failed, exception: " + e.getMessage());
                }
                try {
                    right = convertExprToTrinoTupleDomain(compoundPredicate.getChild(1));
                } catch (AnalysisException e) {
                    LOG.warn("right predicate of compound predicate failed, exception: " + e.getMessage());
                }
                if (left != null && right != null) {
                    return left.intersect(right);
                } else if (left != null) {
                    return left;
                } else if (right != null) {
                    return right;
                }
                throw new AnalysisException("Can not convert both sides of compound predicate ["
                        + compoundPredicate.getOp() + "] to TupleDomain.");
            }
            case OR: {
                TupleDomain<ColumnHandle> left = convertExprToTrinoTupleDomain(compoundPredicate.getChild(0));
                TupleDomain<ColumnHandle> right = convertExprToTrinoTupleDomain(compoundPredicate.getChild(1));
                return TupleDomain.columnWiseUnion(left, right);
            }
            case NOT:
            default:
                throw new AnalysisException("Do not support convert compound predicate [" + compoundPredicate.getOp()
                        + "] to TupleDomain.");
        }
    }

    private TupleDomain<ColumnHandle> inPredicateConverter(InPredicate predicate) throws AnalysisException {
        // Make sure the col slot is always first
        SlotRef slotRef = convertExprToSlotRef(predicate.getChild(0));
        if (slotRef == null) {
            throw new AnalysisException("slotRef is null in inPredicateConverter.");
        }
        String colName = slotRef.getColumnName();
        Type type = trinoConnectorColumnMetadataMap.get(colName).getType();
        List<Range> ranges = Lists.newArrayList();
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            LiteralExpr literalExpr = convertExprToLiteral(predicate.getChild(i));
            if (literalExpr == null) {
                throw new AnalysisException("literalExpr of InPredicate's children is null in inPredicateConverter.");
            }
            ranges.add(Range.equal(type, convertLiteralToDomainValues(type.getClass(), literalExpr)));
        }

        Domain domain = predicate.isNotIn()
                ? Domain.create(ValueSet.all(type).subtract(ValueSet.ofRanges(ranges)), false)
                : Domain.create(ValueSet.ofRanges(ranges), false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
        return tupleDomain;
    }

    private TupleDomain<ColumnHandle> binaryPredicateConverter(BinaryPredicate predicate) throws AnalysisException {
        // Make sure the col slot is always first
        SlotRef slotRef = convertExprToSlotRef(predicate.getChild(0));
        if (slotRef == null) {
            throw new AnalysisException("slotRef is null in binaryPredicateConverter.");
        }
        LiteralExpr literalExpr = convertExprToLiteral(predicate.getChild(1));
        // literalExpr == null means predicate.getChild(1) is not a LiteralExpr or CastExpr
        // such as 'where A.a < A.b'，predicate.getChild(1) is SlotRef
        if (literalExpr == null) {
            throw new AnalysisException("literalExpr of BinaryPredicate's child is null in binaryPredicateConverter.");
        }

        String colName = slotRef.getColumnName();
        Type type = trinoConnectorColumnMetadataMap.get(colName).getType();
        Domain domain = null;
        TExprOpcode opcode = predicate.getOpcode();
        switch (opcode) {
            case EQ:
                domain = Domain.create(ValueSet.ofRanges(Range.equal(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                break;
            case EQ_FOR_NULL: {
                if (literalExpr instanceof NullLiteral) {
                    domain = Domain.onlyNull(type);
                } else {
                    domain = Domain.create(ValueSet.ofRanges(Range.equal(type,
                            convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                }
                break;
            }
            case NE:
                domain = Domain.create(ValueSet.all(type).subtract(ValueSet.ofRanges(Range.equal(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr)))), false);
                break;
            case LT:
                domain = Domain.create(ValueSet.ofRanges(Range.lessThan(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                break;
            case LE:
                domain = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                break;
            case GT:
                domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                break;
            case GE:
                domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type,
                        convertLiteralToDomainValues(type.getClass(), literalExpr))), false);
                break;
            case INVALID_OPCODE:
            default:
                throw new AnalysisException("Do not support opcode [" + opcode + "] in binaryPredicateConverter.");
        }
        return TupleDomain.withColumnDomains(ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
    }

    private TupleDomain<ColumnHandle> isNullPredicateConverter(IsNullPredicate predicate) throws AnalysisException {
        Objects.requireNonNull(predicate.getChild(0), "The first child of IsNullPredicate is null.");
        SlotRef slotRef = convertExprToSlotRef(predicate.getChild(0));
        if (slotRef == null) {
            throw new AnalysisException("slotRef is null in IsNullPredicate.");
        }
        String colName = slotRef.getColumnName();
        Type type = trinoConnectorColumnMetadataMap.get(colName).getType();
        if (predicate.isNotNull()) {
            return TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), Domain.notNull(type)));
        }
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), Domain.onlyNull(type)));
    }

    /* Since different Trino types have different data formats stored in their Range,
       we need to convert the data format stored in Doris's LiteralExpr to the corresponding Java data type
       which can be recognized by the Trino Type Range.
       The correspondence between different Trino types and the Java data types stored in their Range is as follows:

        Trino Type                               Java Type

        BooleanType                              boolean
        TinyintType                              long
        SmallintType                             long
        IntegerType                              long
        BigintType                               long
        RealType                                 long
        ShortDecimalType                         long
        LongDecimalType                          io.trino.spi.type.Int128
        CharType                                 io.airlift.slice.Slice
        VarbinaryType                            io.airlift.slice.Slice
        VarcharType                              io.airlift.slice.Slice
        DateType                                 long
        DoubleType                               double
        TimeType                                 long
        ShortTimestampType                       long
        LongTimestampType                        io.trino.spi.type.LongTimestamp
        ShortTimestampWithTimeZoneType           long
        LongTimestampWithTimeZoneType            io.trino.spi.type.LongTimestampWithTimeZone
        ArrayType                                io.trino.spi.block.Block
        MapType                                  io.trino.spi.block.SqlMap
        RowType                                  io.trino.spi.block.SqlRow*/
    private Object convertLiteralToDomainValues(Class<? extends Type> type, LiteralExpr literalExpr)
            throws AnalysisException {
        switch (type.getSimpleName()) {
            case "BooleanType":
                return literalExpr.getRealValue();
            case "TinyintType":
            case "SmallintType":
            case "IntegerType":
            case "BigintType":
                return literalExpr.getLongValue();
            case "RealType":
                return (long) Float.floatToIntBits((float) literalExpr.getDoubleValue());
            case "DoubleType":
                return literalExpr.getDoubleValue();
            case "ShortDecimalType": {
                BigDecimal value = (BigDecimal) literalExpr.getRealValue();
                BigDecimal tmpValue = new BigDecimal(Math.pow(10, DecimalLiteral.getBigDecimalScale(value)));
                value = value.multiply(tmpValue);
                return value.longValue();
            }
            case "LongDecimalType": {
                BigDecimal value = (BigDecimal) literalExpr.getRealValue();
                BigDecimal tmpValue = new BigDecimal(Math.pow(10, DecimalLiteral.getBigDecimalScale(value)));
                value = value.multiply(tmpValue);
                return Int128.valueOf(value.toBigIntegerExact());
            }
            case "CharType":
            case "VarbinaryType":
            case "VarcharType":
                return Slices.utf8Slice((String) literalExpr.getRealValue());
            case "DateType":
                return ((DateLiteral) literalExpr).daynr() - new DateLiteral(EPOCH_DATE).daynr();
            case "ShortTimestampType": {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                return dateLiteral.unixTimestamp(TimeZone.getTimeZone(GMT)) * 1000
                        + dateLiteral.getMicrosecond();
            }
            case "LongTimestampType": {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                long epochMicros = dateLiteral.unixTimestamp(TimeZone.getTimeZone(GMT)) * 1000
                        + dateLiteral.getMicrosecond();
                return new LongTimestamp(epochMicros, 0);
            }
            case "LongTimestampWithTimeZoneType": {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                long epochMillis = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
                int picosOfMilli = (int) dateLiteral.getMicrosecond() * 1000000;
                TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(TimeUtils.getTimeZone().toZoneId().toString());
                return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, timeZoneKey);
            }
            case "ShortTimestampWithTimeZoneType":
            case "TimeType":
            case "ArrayType":
            case "MapType":
            case "RowType":
            default:
                return new AnalysisException("Do not support convert trino type [" + type.getSimpleName()
                        + "] to domain values.");
        }
    }

    private SlotRef convertExprToSlotRef(Expr expr) {
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

    private LiteralExpr convertExprToLiteral(Expr expr) {
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
