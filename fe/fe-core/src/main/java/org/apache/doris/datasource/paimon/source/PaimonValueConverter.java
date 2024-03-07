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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.TimeZone;

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

    public BinaryString visit(CharType charType) {
        // Currently, Paimon does not support predicate push-down for char
        // ref: org.apache.paimon.predicate.PredicateBuilder.convertJavaObject
        return null;
    }

    public Boolean visit(BooleanType booleanType) {
        if (expr instanceof BoolLiteral) {
            BoolLiteral boolLiteral = (BoolLiteral) expr;
            return boolLiteral.getValue();
        }
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

    public Byte visit(TinyIntType tinyIntType) {
        if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return (byte) intLiteral.getValue();
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

    // when a = 9.1,paimon can get data,doris can not get data
    // when a > 9.1,paimon can not get data,doris can get data
    // paimon is no problem,but we consistent with Doris internal table
    // Therefore, comment out this code
    public Float visit(FloatType floatType) {
        return null;
    }

    public Double visit(DoubleType doubleType) {
        if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            return floatLiteral.getValue();
        }
        return null;
    }

    public Integer visit(DateType dateType) {
        if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            long l = LocalDate.of((int) dateLiteral.getYear(), (int) dateLiteral.getMonth(), (int) dateLiteral.getDay())
                    .toEpochDay();
            return (int) l;
        }
        return null;
    }

    public Timestamp visit(TimestampType timestampType) {
        if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            instance.set((int) dateLiteral.getYear(), (int) (dateLiteral.getMonth() - 1), (int) dateLiteral.getDay(),
                    (int) dateLiteral.getHour(), (int) dateLiteral.getMinute(), (int) dateLiteral.getSecond());
            return Timestamp
                    .fromEpochMillis(instance.getTimeInMillis() / 1000 * 1000 + dateLiteral.getMicrosecond() / 1000);
        }
        return null;
    }

    @Override
    protected Object defaultMethod(DataType dataType) {
        return null;
    }
}
