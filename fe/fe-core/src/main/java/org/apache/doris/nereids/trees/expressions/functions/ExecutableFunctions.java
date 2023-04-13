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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * functions that can be executed in FE.
 * current we support: (IF YOU ADD A NEW FUNCTION, PLEASE ADD IT HERE)
 * add
 * subtract
 * multiply
 * divide
 * date_add
 * date_sub
 * years_add
 * months_add
 * days_add
 * hours_add
 * minutes_add
 * seconds_add
 * years_sub
 * months_sub
 * days_sub
 * hours_sub
 * minutes_sub
 * seconds_sub
 * date
 * datediff
 * date_format
 * date_trunc
 * dateofweek
 * datev2
 * time_round series.
 */
public class ExecutableFunctions {
    public static final ExecutableFunctions INSTANCE = new ExecutableFunctions();
    private static final Random RANDOM = new Random();
    private static final LocalDateTime START_ORIGINAL_DAY = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
    private static final LocalDateTime START_ORIGINAL_WEEK = LocalDateTime.of(1970, 1, 4, 0, 0, 0);

    enum DATE {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND
    }




    /**
     * other scalar function
     */
    @ExecFunction(name = "abs", argTypes = {"TINYINT"}, returnType = "TINYINT")
    public static TinyIntLiteral abs(TinyIntLiteral literal) {
        return new TinyIntLiteral((byte) Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"SMALLINT"}, returnType = "SMALLINT")
    public static SmallIntLiteral abs(SmallIntLiteral literal) {
        return new SmallIntLiteral((short) Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"INT"}, returnType = "INT")
    public static IntegerLiteral abs(IntegerLiteral literal) {
        return new IntegerLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"BIGINT"}, returnType = "BIGINT")
    public static BigIntLiteral abs(BigIntLiteral literal) {
        return new BigIntLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral abs(LargeIntLiteral literal) {
        return new LargeIntLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs", argTypes = {"FLOAT"}, returnType = "FLOAT")
    public static FloatLiteral abs(FloatLiteral literal) {
        return new FloatLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral abs(DoubleLiteral literal) {
        return new DoubleLiteral(Math.abs(literal.getValue()));
    }

    @ExecFunction(name = "abs", argTypes = {"DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral abs(DecimalLiteral literal) {
        return new DecimalLiteral(literal.getValue().abs());
    }

    @ExecFunction(name = "abs", argTypes = {"DECIMALV3"}, returnType = "DECIMALV3")
    public static DecimalV3Literal abs(DecimalV3Literal literal) {
        return new DecimalV3Literal(literal.getValue().abs());
    }

    @ExecFunction(name = "acos", argTypes = {"DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral acos(DoubleLiteral literal) {
        return new DoubleLiteral(Math.acos(literal.getValue()));
    }

    @ExecFunction(name = "append_trailing_if_char_absent", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral appendTrailingIfCharAbsent(VarcharLiteral literal, VarcharLiteral chr) {
        if (literal.getValue().length() != 1) {
            return null;
        }
        return literal.getValue().endsWith(chr.getValue()) ? literal
                : new VarcharLiteral(literal.getValue() + chr.getValue());
    }

    @ExecFunction(name = "e", argTypes = {}, returnType = "DOUBLE")
    public static DoubleLiteral e() { // CHECKSTYLE IGNORE THIS LINE
        return new DoubleLiteral(Math.E);
    }

    @ExecFunction(name = "p1", argTypes = {}, returnType = "DOUBLE")
    public static DoubleLiteral pi() {
        return new DoubleLiteral(Math.PI);
    }

    @ExecFunction(name = "uuid", argTypes = {}, returnType = "VARCHAR")
    public static VarcharLiteral uuid() {
        return new VarcharLiteral(UUID.randomUUID().toString());
    }

    @ExecFunction(name = "rand", argTypes = {}, returnType = "DOUBLE")
    public static DoubleLiteral rand() {
        return new DoubleLiteral(RANDOM.nextDouble());
    }

    @ExecFunction(name = "random", argTypes = {}, returnType = "DOUBLE")
    public static DoubleLiteral random() {
        return new DoubleLiteral(RANDOM.nextDouble());
    }
}
