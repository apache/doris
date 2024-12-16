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

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.trinoconnector.source.TrinoConnectorPredicateConverter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

public class TrinoConnectorPredicateTest {

    private static final ImmutableMap<String, ColumnHandle> trinoConnectorColumnHandleMap =
             new ImmutableMap.Builder()
                     .put("c_bool", new MockColumnHandle("c_bool"))
                     .put("c_tinyint", new MockColumnHandle("c_tinyint"))
                     .put("c_smallint", new MockColumnHandle("c_smallint"))
                     .put("c_int", new MockColumnHandle("c_int"))
                     .put("c_bigint", new MockColumnHandle("c_bigint"))
                     .put("c_real", new MockColumnHandle("c_real"))
                     .put("c_short_decimal", new MockColumnHandle("c_short_decimal"))
                     .put("c_long_decimal", new MockColumnHandle("c_long_decimal"))
                     .put("c_char", new MockColumnHandle("c_char"))
                     .put("c_varchar", new MockColumnHandle("c_varchar"))
                     .put("c_varbinary", new MockColumnHandle("c_varbinary"))
                     .put("c_date", new MockColumnHandle("c_date"))
                     .put("c_double", new MockColumnHandle("c_double"))
                     .put("c_short_timestamp", new MockColumnHandle("c_short_timestamp"))
                     // .put("c_short_timestamp_timezone", new MockColumnHandle("c_short_timestamp_timezone"))
                     .put("c_long_timestamp", new MockColumnHandle("c_long_timestamp"))
                     .put("c_long_timestamp_timezone", new MockColumnHandle("c_long_timestamp_timezone"))
                     .build();

    private static final ImmutableMap<String, ColumnMetadata> trinoConnectorColumnMetadataMap =
            new ImmutableMap.Builder()
                    .put("c_bool", new ColumnMetadata("c_bool", BooleanType.BOOLEAN))
                    .put("c_tinyint", new ColumnMetadata("c_tinyint", TinyintType.TINYINT))
                    .put("c_smallint", new ColumnMetadata("c_smallint", SmallintType.SMALLINT))
                    .put("c_int", new ColumnMetadata("c_int", IntegerType.INTEGER))
                    .put("c_bigint", new ColumnMetadata("c_bigint", BigintType.BIGINT))
                    .put("c_real", new ColumnMetadata("c_real", RealType.REAL))
                    .put("c_short_decimal", new ColumnMetadata("c_short_decimal",
                            DecimalType.createDecimalType(9, 2)))
                    .put("c_long_decimal", new ColumnMetadata("c_long_decimal",
                            DecimalType.createDecimalType(38, 15)))
                    .put("c_char", new ColumnMetadata("c_char", CharType.createCharType(128)))
                    .put("c_varchar", new ColumnMetadata("c_varchar",
                            VarcharType.createVarcharType(128)))
                    .put("c_varbinary", new ColumnMetadata("c_varbinary", VarbinaryType.VARBINARY))
                    .put("c_date", new ColumnMetadata("c_date", DateType.DATE))
                    .put("c_double", new ColumnMetadata("c_double", DoubleType.DOUBLE))
                    .put("c_short_timestamp", new ColumnMetadata("c_short_timestamp",
                            TimestampType.TIMESTAMP_MICROS))
                    // .put("c_short_timestamp_timezone", new ColumnMetadata("c_short_timestamp_timezone",
                    //         TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS))
                    .put("c_long_timestamp", new ColumnMetadata("c_long_timestamp",
                            TimestampType.TIMESTAMP_PICOS))
                    .put("c_long_timestamp_timezone", new ColumnMetadata("c_long_timestamp_timezone",
                            TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS))
                    .build();

    private static TrinoConnectorPredicateConverter trinoConnectorPredicateConverter;

    @BeforeClass
    public static void before() throws AnalysisException {
        trinoConnectorPredicateConverter = new TrinoConnectorPredicateConverter(
                trinoConnectorColumnHandleMap,
                trinoConnectorColumnMetadataMap);
    }

    @Test
    public void testBinaryEqPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct equal binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testBinaryEqualForNullPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct equal binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(Operator.EQ_FOR_NULL, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }

        // test <=>
        SlotRef intSlot = new SlotRef(new TableName("test_table"), "c_int");
        NullLiteral nullLiteral = NullLiteral.create(Type.INT);
        BinaryPredicate expr = new BinaryPredicate(Operator.EQ_FOR_NULL, intSlot, nullLiteral);
        TupleDomain<ColumnHandle> testNullTupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                expr);
        TupleDomain<ColumnHandle> expectNullTupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(trinoConnectorColumnHandleMap.get("c_int"), Domain.onlyNull(IntegerType.INTEGER)));
        Assert.assertTrue(expectNullTupleDomain.contains(testNullTupleDomain));
    }

    @Test
    public void testBinaryLessThanPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.lessThan(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct lessThan binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(Operator.LT, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testBinaryLessEqualPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.lessThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct lessThanOrEqual binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(Operator.LE, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testBinaryGreatThanPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.greaterThan(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct greaterThan binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(Operator.GT, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testBinaryGreaterEqualPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.greaterThanOrEqual(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();
        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain domain = Domain.create(ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), domain));
            expectTupleDomain.add(tupleDomain);
        }

        // test results, construct greaterThanOrEqual binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(Operator.GE, slotRefs.get(i),
                    literalList.get(i));
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }

        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectTupleDomain.size(); i++) {
            Assert.assertTrue(expectTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testInPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // expect results
        List<TupleDomain<ColumnHandle>> expectInTupleDomain = Lists.newArrayList();
        List<TupleDomain<ColumnHandle>> expectNotInTupleDomain = Lists.newArrayList();
        ImmutableList<Range> expectRanges = new ImmutableList.Builder()
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bool").getType(), true))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_tinyint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_smallint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_int").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_bigint").getType(), 1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_real").getType(),
                        Long.valueOf(Float.floatToIntBits(1.23f))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_double").getType(), 3.1415926456))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_decimal").getType(), 12345623L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_decimal").getType(),
                        Int128.valueOf(new BigInteger("12345678901234567890123123"))))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_char").getType(),
                        Slices.utf8Slice("trino connector char test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varchar").getType(),
                        Slices.utf8Slice("trino connector varchar test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_varbinary").getType(),
                        Slices.utf8Slice("trino connector varbinary test")))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_date").getType(), -1L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp").getType(),
                        1000001L))
                // .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_short_timestamp_timezone").getType(),
                //         0L))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp").getType(),
                        new LongTimestamp(1000001L, 0)))
                .add(Range.equal(trinoConnectorColumnMetadataMap.get("c_long_timestamp_timezone").getType(),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1000000,
                                TimeZoneKey.getTimeZoneKey("Asia/Shanghai"))))
                .build();

        for (int i = 0; i < slotRefs.size(); i++) {
            final String colName = slotRefs.get(i).getColumnName();
            Domain inDomain = Domain.create(
                    ValueSet.ofRanges(Lists.newArrayList(expectRanges.get(i))), false);
            Domain notInDomain = Domain.create(ValueSet.all(trinoConnectorColumnMetadataMap.get(colName).getType())
                            .subtract(ValueSet.ofRanges(expectRanges.get(i))), false);
            TupleDomain<ColumnHandle> inTupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), inDomain));
            TupleDomain<ColumnHandle> notInTupleDomain = TupleDomain.withColumnDomains(
                    ImmutableMap.of(trinoConnectorColumnHandleMap.get(colName), notInDomain));
            expectInTupleDomain.add(inTupleDomain);
            expectNotInTupleDomain.add(notInTupleDomain);
        }

        // test results, construct equal binary predicate
        List<TupleDomain<ColumnHandle>> testTupleDomain = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            InPredicate expr = new InPredicate(slotRefs.get(i), literalList.get(i), false);
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }
        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectInTupleDomain.size(); i++) {
            Assert.assertTrue(expectInTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }

        testTupleDomain.clear();
        for (int i = 0; i < slotRefs.size(); i++) {
            InPredicate expr = new InPredicate(slotRefs.get(i), literalList.get(i), true);
            TupleDomain<ColumnHandle> tupleDomain = trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(
                    expr);
            testTupleDomain.add(tupleDomain);
        }
        // verify if `testTupleDomain` is equal to `expectTupleDomain`.
        for (int i = 0; i < expectNotInTupleDomain.size(); i++) {
            Assert.assertTrue(expectNotInTupleDomain.get(i).contains(testTupleDomain.get(i)));
        }
    }

    @Test
    public void testCompoundPredicate() throws AnalysisException {
        // construct slotRefs and literalLists
        List<SlotRef> slotRefs = mockSlotRefs();
        List<LiteralExpr> literalList = mockLiteralExpr();

        // valid expr
        List<Expr> validExprs = Lists.newArrayList();
        for (int i = 0; i < slotRefs.size(); i++) {
            BinaryPredicate expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRefs.get(i),
                    literalList.get(i));
            validExprs.add(expr);
        }

        // invalid expr
        BinaryPredicate invalidExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                literalList.get(0), literalList.get(0));

        // AND
        // valid AND valid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < validExprs.size(); j++) {
                CompoundPredicate andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
                        validExprs.get(i), validExprs.get(j));
                trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(andPredicate);
            }
        }

        // valid AND invalid
        CompoundPredicate andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
                validExprs.get(0), invalidExpr);
        trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(andPredicate);

        // invalid AND valid
        andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, invalidExpr, validExprs.get(0));
        trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(andPredicate);

        // invalid AND invalid
        andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, invalidExpr, invalidExpr);
        try {
            trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(andPredicate);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Can not convert both sides of compound predicate"));
        }

        // OR
        // valid OR valid
        for (int i = 0; i < validExprs.size(); i++) {
            for (int j = 0; j < validExprs.size(); j++) {
                CompoundPredicate orPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR,
                        validExprs.get(i), validExprs.get(j));
                trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(orPredicate);
            }
        }

        // // valid OR valid
        try {
            CompoundPredicate orPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND,
                    validExprs.get(0), invalidExpr);
            trinoConnectorPredicateConverter.convertExprToTrinoTupleDomain(orPredicate);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("slotRef is null in binaryPredicateConverter"));
        }
    }

    private List<SlotRef> mockSlotRefs() {
        return new ImmutableList.Builder()
                .add(new SlotRef(new TableName("test_table"), "c_bool"))

                .add(new SlotRef(new TableName("test_table"), "c_tinyint"))
                .add(new SlotRef(new TableName("test_table"), "c_smallint"))
                .add(new SlotRef(new TableName("test_table"), "c_int"))
                .add(new SlotRef(new TableName("test_table"), "c_bigint"))

                .add(new SlotRef(new TableName("test_table"), "c_real"))
                .add(new SlotRef(new TableName("test_table"), "c_double"))

                .add(new SlotRef(new TableName("test_table"), "c_short_decimal"))
                .add(new SlotRef(new TableName("test_table"), "c_long_decimal"))

                .add(new SlotRef(new TableName("test_table"), "c_char"))
                .add(new SlotRef(new TableName("test_table"), "c_varchar"))
                .add(new SlotRef(new TableName("test_table"), "c_varbinary"))

                .add(new SlotRef(new TableName("test_table"), "c_date"))
                .add(new SlotRef(new TableName("test_table"), "c_short_timestamp"))
                // .add(new SlotRef(new TableName("test_table"), "c_short_timestamp_timezone"))
                .add(new SlotRef(new TableName("test_table"), "c_long_timestamp"))
                .add(new SlotRef(new TableName("test_table"), "c_long_timestamp_timezone"))
                .build();
    }

    private List<LiteralExpr> mockLiteralExpr() throws AnalysisException {
        return new ImmutableList.Builder()
                // boolean
                .add(new BoolLiteral(true))
                // Integer
                .add(new IntLiteral(1, Type.TINYINT))
                .add(new IntLiteral(1, Type.SMALLINT))
                .add(new IntLiteral(1, Type.INT))
                .add(new IntLiteral(1, Type.BIGINT))

                .add(new FloatLiteral(1.23, Type.FLOAT)) // Real type
                .add(new FloatLiteral(3.1415926456, Type.DOUBLE))

                .add(new DecimalLiteral(new BigDecimal("123456.23")))
                .add(new DecimalLiteral(new BigDecimal("12345678901234567890123.123")))

                .add(new StringLiteral("trino connector char test"))
                .add(new StringLiteral("trino connector varchar test"))
                .add(new StringLiteral("trino connector varbinary test"))

                .add(new DateLiteral("1969-12-31", Type.DATEV2))
                .add(new DateLiteral("1970-01-01 00:00:01.000001", Type.DATETIMEV2))
                // .add(new DateLiteral("1970-01-01 00:00:00.000000", Type.DATETIMEV2))
                .add(new DateLiteral("1970-01-01 00:00:01.000001", Type.DATETIMEV2))
                .add(new DateLiteral("1970-01-01 08:00:01.000001", Type.DATETIMEV2))
                .build();
    }

    private static class MockColumnHandle implements ColumnHandle {
        private String colName;

        MockColumnHandle(String colName) {
            this.colName = colName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MockColumnHandle that = (MockColumnHandle) o;
            return colName.equals(that.colName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(colName);
        }
    }
}
