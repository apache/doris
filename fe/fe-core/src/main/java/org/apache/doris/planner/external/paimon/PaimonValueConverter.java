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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Convert LiteralExpr to paimon value.
 */
public class PaimonValueConverter extends DataTypeDefaultVisitor<Object> {
    private LiteralExpr expr;

    public PaimonValueConverter(LiteralExpr expr) {
        this.expr = expr;
    }

    public BinaryString visit(VarCharType varCharType) {
        return BinaryString.fromString(expr.getStringValue());
    }

    public Boolean visit(BooleanType booleanType) {
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        }
        return null;
    }

    public Object visit(BinaryType binaryType) {
        return null;
    }

    public Decimal visit(DecimalType decimalType) {
        if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            BigDecimal value = decimalLiteral.getValue();
            return Decimal.fromBigDecimal(value, value.precision(), value.scale());
        }
        return null;
    }

    public Short visit(SmallIntType smallIntType) {
        if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return (short) intLiteral.getValue();
        }
        return null;
    }

    public Integer visit(IntType intType) {
        if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return (int) intLiteral.getValue();
        }
        return null;
    }

    public Long visit(BigIntType bigIntType) {
        if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return intLiteral.getValue();
        }
        return null;
    }

    public Float visit(FloatType floatType) {
        if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return (float) floatLiteral.getValue();
        }
        return null;
    }

    public Double visit(DoubleType doubleType) {
        if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        }
        return null;
    }

    public Object visit(DateType dateType) {
        return null;
    }

    public Timestamp visit(TimestampType timestampType) {
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
    }

    public Object visit(ArrayType arrayType) {
        return null;
    }

    public Object visit(MapType mapType) {
        return null;
    }

    public Object visit(RowType rowType) {
        return null;
    }

    @Override
    protected Object defaultMethod(DataType dataType) {
        return null;
    }
}

