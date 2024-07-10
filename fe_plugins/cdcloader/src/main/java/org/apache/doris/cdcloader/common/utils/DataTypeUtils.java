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

package org.apache.doris.cdcloader.common.utils;

import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

public class DataTypeUtils {

    private static final String BOOLEAN = "BOOLEAN";
    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String LARGEINT = "LARGEINT";
    private static final String FLOAT = "FLOAT";
    private static final String DOUBLE = "DOUBLE";
    private static final String DECIMAL = "DECIMAL";
    private static final String DATE = "DATE";
    private static final String DATETIME = "DATETIME";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String STRING = "STRING";
    private static final String ARRAY = "ARRAY";
    private static final String JSONB = "JSONB";
    private static final String JSON = "JSON";
    private static final String MAP = "MAP";
    private static final String STRUCT = "STRUCT";
    private static final String VARIANT = "VARIANT";
    private static final String IPV4 = "IPV4";
    private static final String IPV6 = "IPV6";

    /** Max size of char type of Doris. */
    public static final int MAX_CHAR_SIZE = 255;
    /** Max size of varchar type of Doris. */
    public static final int MAX_VARCHAR_SIZE = 65533;
    /* Max precision of datetime type of Doris. */
    public static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;

    public static String convertFromDataType(DataType dataType, boolean isKey){
        return dataType.accept(new DorisDataTypeVisitor(isKey));
    }

    private static class DorisDataTypeVisitor extends DataTypeDefaultVisitor<String> {

        private  boolean isKey;

        public DorisDataTypeVisitor(boolean isKey) {
            this.isKey = isKey;
        }

        @Override
        public String visit(CharType charType) {
            long length = charType.getLength() * 3L;
            if (length <= MAX_CHAR_SIZE) {
                return String.format("%s(%s)", CHAR, length);
            } else {
                return visit(new VarCharType(charType.getLength()));
            }
        }

        @Override
        public String visit(VarCharType varCharType) {
            // Flink varchar length max value is int, it may overflow after multiplying by 3
            long length = varCharType.getLength() * 3L;
            if(length >= MAX_VARCHAR_SIZE && !isKey){
                return STRING;
            }else{
                return String.format("%s(%s)", VARCHAR, Math.min(length, MAX_VARCHAR_SIZE));
            }
        }

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(BinaryType binaryType) {
            return STRING;
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            return STRING;
        }

        @Override
        public String visit(DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            return String.format("%s(%s,%s)", DECIMAL, precision, Math.max(scale, 0));
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

        @Override
        public String visit(DateType dateType) {
            return DATE;
        }

        @Override
        public String visit(TimeType timeType) {
            return STRING;
        }

        @Override
        public String visit(TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            return String.format("%s(%s)", DATETIME, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(ZonedTimestampType zonedTimestampType) {
            int precision = zonedTimestampType.getPrecision();
            return String.format("%s(%s)", DATETIME, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = localZonedTimestampType.getPrecision();
            return String.format("%s(%s)", DATETIME, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(ArrayType arrayType) {
            return STRING;
        }

        @Override
        public String visit(MapType mapType) {
            return STRING;
        }

        @Override
        public String visit(RowType rowType) {
            return STRING;
        }

        @Override
        protected String defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                String.format(
                    "Doesn't support converting type %s to Doris type yet.",
                    dataType.toString()));
        }
    }

}
