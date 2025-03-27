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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

class CompareLiteralTest extends TestWithFeService {

    @Test
    public void testScalar() {
        // boolean type
        checkCompareSameType(0, BooleanLiteral.of(true), BooleanLiteral.of(true));
        checkCompareSameType(1, BooleanLiteral.of(true), BooleanLiteral.of(false));
        checkCompareDiffType(BooleanLiteral.of(true), new IntegerLiteral(0));
        checkCompareDiffType(BooleanLiteral.of(true), new DoubleLiteral(0.5));
        checkCompareDiffType(BooleanLiteral.of(true), new DecimalLiteral(new BigDecimal("0.5")));
        checkCompareDiffType(BooleanLiteral.of(true), new IntegerLiteral(1));
        checkCompareDiffType(BooleanLiteral.of(true), new DoubleLiteral(1.0));
        checkCompareDiffType(BooleanLiteral.of(true), new DecimalLiteral(new BigDecimal("1.0")));
        checkCompareDiffType(BooleanLiteral.of(true), new StringLiteral("tru"));
        checkCompareDiffType(BooleanLiteral.of(true), new StringLiteral("true"));
        checkCompareDiffType(BooleanLiteral.of(true), new StringLiteral("truea"));
        checkCompareSameType(0, BooleanLiteral.of(false), BooleanLiteral.of(false));
        checkCompareDiffType(BooleanLiteral.of(false), new IntegerLiteral(0));
        checkCompareDiffType(BooleanLiteral.of(false), new DoubleLiteral(0.5));
        checkCompareDiffType(BooleanLiteral.of(false), new DecimalLiteral(new BigDecimal("0.5")));
        checkCompareDiffType(BooleanLiteral.of(false), new IntegerLiteral(1));
        checkCompareDiffType(BooleanLiteral.of(false), new DoubleLiteral(1.0));
        checkCompareDiffType(BooleanLiteral.of(false), new DecimalLiteral(new BigDecimal("1.0")));
        checkCompareDiffType(BooleanLiteral.of(false), new StringLiteral("fals"));
        checkCompareDiffType(BooleanLiteral.of(false), new StringLiteral("false"));
        checkCompareDiffType(BooleanLiteral.of(false), new StringLiteral("falsea"));

        // numeric type
        checkCompareSameType(0, new TinyIntLiteral((byte) 127), new TinyIntLiteral((byte) 127));
        checkCompareSameType(0, new TinyIntLiteral((byte) 127), new DoubleLiteral(127.0));
        checkCompareSameType(0, new TinyIntLiteral((byte) 127), new DecimalLiteral(new BigDecimal("127.0")));
        checkCompareSameType(0, new TinyIntLiteral((byte) 127), new DecimalV3Literal(new BigDecimal("127.0")));
        checkCompareDiffType(new TinyIntLiteral((byte) 127), new StringLiteral("12"));
        checkCompareDiffType(new TinyIntLiteral((byte) 127), new StringLiteral("127"));
        checkCompareDiffType(new TinyIntLiteral((byte) 127), new StringLiteral("127.0"));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new SmallIntLiteral((short) 32767));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new DoubleLiteral(32767.0));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new DecimalLiteral(new BigDecimal("32767")));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new DecimalV3Literal(new BigDecimal("32767")));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new IntegerLiteral(2147483647));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new BigIntLiteral(9223372036854775807L));
        checkCompareSameType(-1, new TinyIntLiteral((byte) 127), new LargeIntLiteral(new BigInteger("9223372036854775808")));

        checkCompareSameType(0, new SmallIntLiteral((short) 32767), new SmallIntLiteral((short) 32767));
        checkCompareSameType(0, new SmallIntLiteral((short) 32767), new DoubleLiteral(32767.0));
        checkCompareSameType(0, new SmallIntLiteral((short) 32767), new DecimalLiteral(new BigDecimal("32767.0")));
        checkCompareSameType(0, new SmallIntLiteral((short) 32767), new DecimalV3Literal(new BigDecimal("32767.0")));
        checkCompareDiffType(new SmallIntLiteral((short) 32767), new StringLiteral("3276"));
        checkCompareDiffType(new SmallIntLiteral((short) 32767), new StringLiteral("32767"));
        checkCompareDiffType(new SmallIntLiteral((short) 32767), new StringLiteral("32767.0"));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new IntegerLiteral(2147483647));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new DoubleLiteral(2147483647.0));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new DecimalLiteral(new BigDecimal("2147483647")));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new DecimalV3Literal(new BigDecimal("2147483647")));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new BigIntLiteral(9223372036854775807L));
        checkCompareSameType(-1, new SmallIntLiteral((short) 32767), new LargeIntLiteral(new BigInteger("9223372036854775808")));

        checkCompareSameType(0, new IntegerLiteral(2147483647), new IntegerLiteral(2147483647));
        checkCompareSameType(0, new IntegerLiteral(2147483647), new DoubleLiteral(2147483647.0));
        checkCompareSameType(0, new IntegerLiteral(2147483647), new DecimalLiteral(new BigDecimal("2147483647.0")));
        checkCompareSameType(0, new IntegerLiteral(2147483647), new DecimalV3Literal(new BigDecimal("2147483647.0")));
        checkCompareDiffType(new IntegerLiteral(2147483647), new StringLiteral("214748364"));
        checkCompareDiffType(new IntegerLiteral(2147483647), new StringLiteral("2147483647"));
        checkCompareDiffType(new IntegerLiteral(2147483647), new StringLiteral("2147483647.0"));
        checkCompareSameType(-1, new IntegerLiteral(2147483647), new DecimalLiteral(new BigDecimal("922337203685477580")));
        checkCompareSameType(-1, new IntegerLiteral(2147483647), new DecimalV3Literal(new BigDecimal("9223372036854775807")));
        checkCompareSameType(-1, new IntegerLiteral(2147483647), new BigIntLiteral(9223372036854775807L));
        checkCompareSameType(-1, new IntegerLiteral(2147483647), new LargeIntLiteral(new BigInteger("9223372036854775808")));

        checkCompareSameType(0, new BigIntLiteral(9223372036854775807L), new BigIntLiteral(9223372036854775807L));
        checkCompareSameType(0, new BigIntLiteral(922337203685477580L), new DecimalLiteral(new BigDecimal("922337203685477580.0")));
        checkCompareSameType(0, new BigIntLiteral(9223372036854775807L), new DecimalV3Literal(new BigDecimal("9223372036854775807.0")));
        checkCompareDiffType(new BigIntLiteral(9223372036854775807L), new StringLiteral("922337203685477580"));
        checkCompareDiffType(new BigIntLiteral(9223372036854775807L), new StringLiteral("9223372036854775807"));
        checkCompareDiffType(new BigIntLiteral(9223372036854775807L), new StringLiteral("9223372036854775807.0"));
        checkCompareSameType(-1, new BigIntLiteral(922337203685477587L), new DecimalLiteral(new BigDecimal("922337203685477588")));
        checkCompareSameType(-1, new BigIntLiteral(9223372036854775807L), new DecimalV3Literal(new BigDecimal("9223372036854775808")));
        checkCompareSameType(-1, new BigIntLiteral(9223372036854775807L), new LargeIntLiteral(new BigInteger("9223372036854775808")));

        checkCompareSameType(0, new LargeIntLiteral(new BigInteger("9223372036854775808")), new LargeIntLiteral(new BigInteger("9223372036854775808")));
        checkCompareSameType(0, new LargeIntLiteral(new BigInteger("922337203685477588")), new DecimalLiteral(new BigDecimal("922337203685477588.0")));
        checkCompareSameType(0, new LargeIntLiteral(new BigInteger("9223372036854775808")), new DecimalV3Literal(new BigDecimal("9223372036854775808.0")));
        checkCompareDiffType(new LargeIntLiteral(new BigInteger("9223372036854775808")), new StringLiteral("922337203685477580"));
        checkCompareDiffType(new LargeIntLiteral(new BigInteger("9223372036854775808")), new StringLiteral("9223372036854775808"));
        checkCompareDiffType(new LargeIntLiteral(new BigInteger("9223372036854775808")), new StringLiteral("9223372036854775808.0"));
        checkCompareSameType(-1, new LargeIntLiteral(new BigInteger("922337203685477588")), new DecimalLiteral(new BigDecimal("922337203685477589")));
        checkCompareSameType(-1, new LargeIntLiteral(new BigInteger("9223372036854775808")), new DecimalV3Literal(new BigDecimal("9223372036854775809")));

        checkCompareSameType(0, new FloatLiteral(100.5f), new FloatLiteral(100.5f));
        checkCompareSameType(0, new FloatLiteral(100.5f), new DoubleLiteral(100.5));
        checkCompareSameType(0, new FloatLiteral(100.5f), new DecimalLiteral(new BigDecimal("100.5")));
        checkCompareSameType(0, new FloatLiteral(100.5f), new DecimalV3Literal(new BigDecimal("100.5")));
        checkCompareDiffType(new FloatLiteral(100.0f), new StringLiteral("100"));
        checkCompareDiffType(new FloatLiteral(100.0f), new StringLiteral("100.0"));
        checkCompareDiffType(new FloatLiteral(100.0f), new StringLiteral("100.00"));
        checkCompareSameType(-1, new FloatLiteral(100.5f), new FloatLiteral(100.6f));
        checkCompareSameType(-1, new FloatLiteral(100.5f), new DoubleLiteral(100.6));
        checkCompareSameType(-1, new FloatLiteral(100.5f), new DecimalLiteral(new BigDecimal("100.6")));
        checkCompareSameType(-1, new FloatLiteral(100.5f), new DecimalV3Literal(new BigDecimal("100.6")));

        checkCompareSameType(0, new DoubleLiteral(100.5), new DoubleLiteral(100.5));
        checkCompareSameType(0, new DoubleLiteral(100.5), new DoubleLiteral(100.5));
        checkCompareSameType(0, new DoubleLiteral(100.5), new DecimalLiteral(new BigDecimal("100.5")));
        checkCompareSameType(0, new DoubleLiteral(100.5), new DecimalV3Literal(new BigDecimal("100.5")));
        checkCompareDiffType(new DoubleLiteral(100.0), new StringLiteral("100"));
        checkCompareDiffType(new DoubleLiteral(100.0), new StringLiteral("100.0"));
        checkCompareDiffType(new DoubleLiteral(100.0), new StringLiteral("100.00"));
        checkCompareSameType(-1, new DoubleLiteral(100.5), new FloatLiteral(100.6f));
        checkCompareSameType(-1, new DoubleLiteral(100.5), new DoubleLiteral(100.6));
        checkCompareSameType(-1, new DoubleLiteral(100.5), new DecimalLiteral(new BigDecimal("100.6")));
        checkCompareSameType(-1, new DoubleLiteral(100.5), new DecimalV3Literal(new BigDecimal("100.6")));

        checkCompareSameType(0, new DecimalLiteral(new BigDecimal("100.5")), new DoubleLiteral(100.5));
        checkCompareSameType(0, new DecimalLiteral(new BigDecimal("100.5")), new DecimalLiteral(new BigDecimal("100.5")));
        checkCompareSameType(0, new DecimalLiteral(new BigDecimal("100.5")), new DecimalV3Literal(new BigDecimal("100.5")));
        checkCompareDiffType(new DecimalLiteral(new BigDecimal("100.0")), new StringLiteral("100"));
        checkCompareDiffType(new DecimalLiteral(new BigDecimal("100.0")), new StringLiteral("100.0"));
        checkCompareDiffType(new DecimalLiteral(new BigDecimal("100.0")), new StringLiteral("100.00"));
        checkCompareSameType(-1, new DecimalLiteral(new BigDecimal("100.5")), new FloatLiteral(100.6f));
        checkCompareSameType(-1, new DecimalLiteral(new BigDecimal("100.5")), new DoubleLiteral(100.6));
        checkCompareSameType(-1, new DecimalLiteral(new BigDecimal("100.5")), new DecimalLiteral(new BigDecimal("100.6")));
        checkCompareSameType(-1, new DecimalLiteral(new BigDecimal("100.5")), new DecimalV3Literal(new BigDecimal("100.6")));

        checkCompareSameType(0, new DecimalV3Literal(new BigDecimal("100.5")), new DoubleLiteral(100.5));
        checkCompareSameType(0, new DecimalV3Literal(new BigDecimal("100.5")), new DecimalLiteral(new BigDecimal("100.5")));
        checkCompareSameType(0, new DecimalV3Literal(new BigDecimal("100.5")), new DecimalV3Literal(new BigDecimal("100.5")));
        checkCompareDiffType(new DecimalV3Literal(new BigDecimal("100.0")), new StringLiteral("100"));
        checkCompareDiffType(new DecimalV3Literal(new BigDecimal("100.0")), new StringLiteral("100.0"));
        checkCompareDiffType(new DecimalV3Literal(new BigDecimal("100.0")), new StringLiteral("100.00"));
        checkCompareSameType(-1, new DecimalV3Literal(new BigDecimal("100.5")), new FloatLiteral(100.6f));
        checkCompareSameType(-1, new DecimalV3Literal(new BigDecimal("100.5")), new DoubleLiteral(100.6));
        checkCompareSameType(-1, new DecimalV3Literal(new BigDecimal("100.5")), new DecimalLiteral(new BigDecimal("100.6")));
        checkCompareSameType(-1, new DecimalV3Literal(new BigDecimal("100.5")), new DecimalV3Literal(new BigDecimal("100.6")));

        // date type
        checkCompareSameType(0, new DateLiteral("2020-01-10"), new DateLiteral("2020-01-10"));
        checkCompareSameType(0, new DateLiteral("2020-01-10"), new DateV2Literal("2020-01-10"));
        checkCompareSameType(0, new DateLiteral("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:00"));
        checkCompareSameType(0, new DateLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.00"));
        checkCompareSameType(1, new DateLiteral("2020-01-10"), new DateLiteral("2020-01-09"));
        checkCompareSameType(1, new DateLiteral("2020-01-10"), new DateV2Literal("2020-01-09"));
        checkCompareSameType(1, new DateLiteral("2020-01-10"), new DateTimeLiteral("2020-01-09 00:00:00"));
        checkCompareSameType(1, new DateLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-09 00:00:00.00"));
        checkCompareSameType(-1, new DateLiteral("2020-01-10"), new DateLiteral("2020-01-11"));
        checkCompareSameType(-1, new DateLiteral("2020-01-10"), new DateV2Literal("2020-01-11"));
        checkCompareSameType(-1, new DateLiteral("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:01"));
        checkCompareSameType(-1, new DateLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.01"));
        checkCompareDiffType(new DateLiteral("2020-01-10"), new StringLiteral("2020-01-1"));
        checkCompareDiffType(new DateLiteral("2020-01-10"), new StringLiteral("2020-01-10"));
        checkCompareDiffType(new DateLiteral("2020-01-10"), new StringLiteral("2020-01-10 "));
        checkCompareSameType(0, new DateV2Literal("2020-01-10"), new DateLiteral("2020-01-10"));
        checkCompareSameType(0, new DateV2Literal("2020-01-10"), new DateV2Literal("2020-01-10"));
        checkCompareSameType(0, new DateV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:00"));
        checkCompareSameType(0, new DateV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.00"));
        checkCompareSameType(1, new DateV2Literal("2020-01-10"), new DateLiteral("2020-01-09"));
        checkCompareSameType(1, new DateV2Literal("2020-01-10"), new DateV2Literal("2020-01-09"));
        checkCompareSameType(1, new DateV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-09 00:00:00"));
        checkCompareSameType(1, new DateV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-09 00:00:00.00"));
        checkCompareSameType(-1, new DateV2Literal("2020-01-10"), new DateLiteral("2020-01-11"));
        checkCompareSameType(-1, new DateV2Literal("2020-01-10"), new DateV2Literal("2020-01-11"));
        checkCompareSameType(-1, new DateV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:01"));
        checkCompareSameType(-1, new DateV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.01"));
        checkCompareDiffType(new DateV2Literal("2020-01-10"), new StringLiteral("2020-01-1"));
        checkCompareDiffType(new DateV2Literal("2020-01-10"), new StringLiteral("2020-01-10"));
        checkCompareDiffType(new DateV2Literal("2020-01-10"), new StringLiteral("2020-01-10 "));
        checkCompareSameType(0, new DateTimeLiteral("2020-01-10"), new DateLiteral("2020-01-10"));
        checkCompareSameType(0, new DateTimeLiteral("2020-01-10"), new DateV2Literal("2020-01-10"));
        checkCompareSameType(0, new DateTimeLiteral("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:00"));
        checkCompareSameType(0, new DateTimeLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.00"));
        checkCompareSameType(1, new DateTimeLiteral("2020-01-10"), new DateLiteral("2020-01-09"));
        checkCompareSameType(1, new DateTimeLiteral("2020-01-10"), new DateV2Literal("2020-01-09"));
        checkCompareSameType(1, new DateTimeLiteral("2020-01-10"), new DateTimeLiteral("2020-01-09 00:00:00"));
        checkCompareSameType(1, new DateTimeLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-09 00:00:00.00"));
        checkCompareSameType(-1, new DateTimeLiteral("2020-01-10"), new DateLiteral("2020-01-11"));
        checkCompareSameType(-1, new DateTimeLiteral("2020-01-10"), new DateV2Literal("2020-01-11"));
        checkCompareSameType(-1, new DateTimeLiteral("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:01"));
        checkCompareSameType(-1, new DateTimeLiteral("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.01"));
        checkCompareDiffType(new DateTimeLiteral("2020-01-10"), new StringLiteral("2020-01-10 00:00"));
        checkCompareDiffType(new DateTimeLiteral("2020-01-10"), new StringLiteral("2020-01-10 00:00:00"));
        checkCompareDiffType(new DateTimeLiteral("2020-01-10"), new StringLiteral("2020-01-10 00:00:00.000"));
        checkCompareSameType(0, new DateTimeV2Literal("2020-01-10"), new DateLiteral("2020-01-10"));
        checkCompareSameType(0, new DateTimeV2Literal("2020-01-10"), new DateV2Literal("2020-01-10"));
        checkCompareSameType(0, new DateTimeV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:00"));
        checkCompareSameType(0, new DateTimeV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.00"));
        checkCompareSameType(1, new DateTimeV2Literal("2020-01-10"), new DateLiteral("2020-01-09"));
        checkCompareSameType(1, new DateTimeV2Literal("2020-01-10"), new DateV2Literal("2020-01-09"));
        checkCompareSameType(1, new DateTimeV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-09 00:00:00"));
        checkCompareSameType(1, new DateTimeV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-09 00:00:00.00"));
        checkCompareSameType(-1, new DateTimeV2Literal("2020-01-10"), new DateLiteral("2020-01-11"));
        checkCompareSameType(-1, new DateTimeV2Literal("2020-01-10"), new DateV2Literal("2020-01-11"));
        checkCompareSameType(-1, new DateTimeV2Literal("2020-01-10"), new DateTimeLiteral("2020-01-10 00:00:01"));
        checkCompareSameType(-1, new DateTimeV2Literal("2020-01-10"), new DateTimeV2Literal("2020-01-10 00:00:00.01"));
        checkCompareDiffType(new DateTimeV2Literal("2020-01-10"), new StringLiteral("2020-01-10 00:00:0"));
        checkCompareDiffType(new DateTimeV2Literal("2020-01-10"), new StringLiteral("2020-01-10 00:00:00"));
        checkCompareDiffType(new DateTimeV2Literal("2020-01-10"), new StringLiteral("2020-01-10 00:00:00.00"));

        // string type
        checkCompareSameType(0, new CharLiteral("abc", -1), new CharLiteral("abc", -1));
        checkCompareSameType(1, new CharLiteral("abc", -1), new CharLiteral("abc", 1));
        checkCompareSameType(0, new CharLiteral("abc", -1), new StringLiteral("abc"));
        checkCompareSameType(0, new CharLiteral("abc", -1), new VarcharLiteral("abc"));
        checkCompareSameType(0, new CharLiteral("abc", -1), new VarcharLiteral("abc", -1));
        checkCompareSameType(1, new CharLiteral("abc", -1), new VarcharLiteral("abc", 1));
        checkCompareSameType(1, new CharLiteral("abc", -1), new CharLiteral("ab", -1));
        checkCompareSameType(1, new CharLiteral("abc", -1), new StringLiteral("ab"));
        checkCompareSameType(1, new CharLiteral("abc", -1), new VarcharLiteral("ab"));
        checkCompareSameType(1, new CharLiteral("abc", -1), new VarcharLiteral("ab", -1));
        checkCompareSameType(-1, new CharLiteral("abc", -1), new CharLiteral("abcd", -1));
        checkCompareSameType(0, new CharLiteral("abc", -1), new CharLiteral("abcd", 3));
        checkCompareSameType(-1, new CharLiteral("abc", -1), new StringLiteral("abcd"));
        checkCompareSameType(-1, new CharLiteral("abc", -1), new VarcharLiteral("abcd"));
        checkCompareSameType(-1, new CharLiteral("abc", -1), new VarcharLiteral("abcd", -1));
        checkCompareSameType(0, new CharLiteral("abc", -1), new VarcharLiteral("abcd", 3));

        // ip type
        checkCompareSameType(0, new IPv4Literal("170.0.0.100"), new IPv4Literal("170.0.0.100"));
        checkCompareSameType(1, new IPv4Literal("170.0.0.100"), new IPv4Literal("160.0.0.200"));
        checkCompareDiffType(new IPv4Literal("172.0.0.100"), new IPv6Literal("1080:0:0:0:8:800:200C:417A"));
        checkCompareSameType(0, new IPv6Literal("1080:0:0:0:8:800:200C:417A"), new IPv6Literal("1080:0:0:0:8:800:200C:417A"));
        checkCompareSameType(1, new IPv6Literal("1080:0:0:0:8:800:200C:417A"), new IPv6Literal("1000:0:0:0:8:800:200C:41AA"));
        IPv4Literal ipv4 = new IPv4Literal("170.0.0.100");
        Assertions.assertEquals(ipv4, new IPv4Literal(ipv4.toLegacyLiteral().getStringValue()));
        IPv6Literal ipv6 = new IPv6Literal("1080:0:0:0:8:800:200C:417A");
        Assertions.assertEquals(ipv6, new IPv6Literal(ipv6.toLegacyLiteral().getStringValue()));

        // null type and max type
        Assertions.assertEquals(0, (new NullLiteral(IntegerType.INSTANCE)).compareTo(new NullLiteral(IntegerType.INSTANCE)));
        Assertions.assertEquals(-1, (new NullLiteral(IntegerType.INSTANCE)).compareTo(new MaxLiteral(IntegerType.INSTANCE)));
        Assertions.assertEquals(0, (new MaxLiteral(IntegerType.INSTANCE)).compareTo(new MaxLiteral(IntegerType.INSTANCE)));
    }

    @Test
    public void testComplex() throws Exception {
        // array type
        checkCompareSameType(0,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkCompareSameType(1,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(200))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkCompareSameType(1,
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200), new IntegerLiteral(1))),
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(100), new IntegerLiteral(200))));
        checkComparableNoException("select array(1,2) = array(1, 2)");
        checkComparableNoException("select array(1,2) > array(1, 2)");

        // json type
        checkNotComparable("select cast ('[1, 2]' as json) = cast('[1, 2]' as json)",
                "comparison predicate could not contains json type");
        checkNotComparable("select cast('[1, 2]' as json) > cast('[1, 2]' as json)",
                "comparison predicate could not contains json type");

        // map type
        checkNotComparable("select map(1, 2) = map(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select map(1, 2) > map(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select cast('(1, 2)' as map<int, int>) = cast('(1, 2)' as map<int, int>)",
                "comparison predicate could not contains complex type");

        // struct type
        checkNotComparable("select struct(1, 2) = struct(1, 2)",
                "comparison predicate could not contains complex type");
        checkNotComparable("select struct(1, 2) > struct(1, 2)",
                "comparison predicate could not contains complex type");
    }

    private void checkCompareSameType(int expect, ComparableLiteral left, ComparableLiteral right) {
        Assertions.assertEquals(expect, left.compareTo(right));
        Assertions.assertEquals(- expect, right.compareTo(left));
        if (((Literal) left).dataType.equals(((Literal) right).dataType)
                && !(left instanceof IPv4Literal) && !(left instanceof IPv6Literal)) {
            Assertions.assertEquals(expect, ((Literal) left).toLegacyLiteral()
                    .compareTo(((Literal) right).toLegacyLiteral()));
            Assertions.assertEquals(- expect, ((Literal) right).toLegacyLiteral()
                    .compareTo(((Literal) left).toLegacyLiteral()));
        }

        Assertions.assertEquals(1, left.compareTo(new NullLiteral(IntegerType.INSTANCE)));
        Assertions.assertEquals(-1, left.compareTo(new MaxLiteral(IntegerType.INSTANCE)));
    }

    private void checkCompareDiffType(ComparableLiteral left, ComparableLiteral right) {
        Assertions.assertThrowsExactly(RuntimeException.class, () -> left.compareTo(right));
        Assertions.assertThrowsExactly(RuntimeException.class, () -> right.compareTo(left));
    }

    private void checkComparableNoException(String sql) throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> executeSql(sql));
    }

    private void checkNotComparable(String sql, String expectErrMsg) throws Exception {
        ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, expectErrMsg,
                () -> executeSql(sql));
    }
}
