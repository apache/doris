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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnType.Type;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Convert paimon type to doris type.
 */
public class PaimonTypeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonTypeUtils.class);

    private PaimonTypeUtils() {
    }

    public static ColumnType fromPaimonType(String columnName, DataType type) {
        PaimonColumnType paimonColumnType = type.accept(PaimonToDorisTypeVisitor.INSTANCE);
        ColumnType columnType = new ColumnType(columnName, paimonColumnType.getType(), paimonColumnType.getLength(),
                paimonColumnType.getPrecision(),
                paimonColumnType.getScale());
        buildComplexColumnType(paimonColumnType, columnType);
        return columnType;
    }

    private static void buildComplexColumnType(PaimonColumnType paimonColumnType, ColumnType columnType) {
        if (CollectionUtils.isNotEmpty(paimonColumnType.getChildTypes())) {
            List<ColumnType> childColumnTypes = paimonColumnType.getChildTypes().stream().map(
                    childPaimonType -> {
                        ColumnType childColumnType = new ColumnType("child", childPaimonType.getType(),
                                childPaimonType.getLength(),
                                childPaimonType.getPrecision(),
                                childPaimonType.getScale());
                        buildComplexColumnType(childPaimonType, childColumnType);
                        return childColumnType;
                    }).collect(Collectors.toList());
            columnType.setChildTypes(childColumnTypes);
        }
    }

    private static class PaimonToDorisTypeVisitor extends DataTypeDefaultVisitor<PaimonColumnType> {

        private static final PaimonToDorisTypeVisitor INSTANCE = new PaimonToDorisTypeVisitor();

        @Override
        public PaimonColumnType visit(CharType charType) {
            return new PaimonColumnType(Type.CHAR, charType.getLength());
        }

        @Override
        public PaimonColumnType visit(VarCharType varCharType) {
            return new PaimonColumnType(Type.VARCHAR, varCharType.getLength());
        }

        @Override
        public PaimonColumnType visit(BooleanType booleanType) {
            return new PaimonColumnType(Type.BOOLEAN);
        }

        @Override
        public PaimonColumnType visit(BinaryType binaryType) {
            return new PaimonColumnType(Type.BINARY);
        }

        @Override
        public PaimonColumnType visit(VarBinaryType varBinaryType) {
            return new PaimonColumnType(Type.BINARY);
        }

        @Override
        public PaimonColumnType visit(DecimalType decimalType) {
            return new PaimonColumnType(Type.DECIMAL128, decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public PaimonColumnType visit(TinyIntType tinyIntType) {
            return new PaimonColumnType(Type.TINYINT);
        }

        @Override
        public PaimonColumnType visit(SmallIntType smallIntType) {
            return new PaimonColumnType(Type.SMALLINT);
        }

        @Override
        public PaimonColumnType visit(IntType intType) {
            return new PaimonColumnType(Type.INT);
        }

        @Override
        public PaimonColumnType visit(BigIntType bigIntType) {
            return new PaimonColumnType(Type.BIGINT);
        }

        @Override
        public PaimonColumnType visit(FloatType floatType) {
            return new PaimonColumnType(Type.FLOAT);
        }

        @Override
        public PaimonColumnType visit(DoubleType doubleType) {
            return new PaimonColumnType(Type.DOUBLE);
        }

        @Override
        public PaimonColumnType visit(DateType dateType) {
            return new PaimonColumnType(Type.DATEV2);
        }

        @Override
        public PaimonColumnType visit(TimeType timeType) {
            PaimonColumnType paimonColumnType = new PaimonColumnType(Type.DATETIMEV2);
            paimonColumnType.setPrecision(timeType.getPrecision());
            return paimonColumnType;
        }

        @Override
        public PaimonColumnType visit(TimestampType timestampType) {
            PaimonColumnType paimonColumnType = new PaimonColumnType(Type.DATETIMEV2);
            paimonColumnType.setPrecision(timestampType.getPrecision());
            return paimonColumnType;
        }

        @Override
        public PaimonColumnType visit(LocalZonedTimestampType localZonedTimestampType) {
            PaimonColumnType paimonColumnType = new PaimonColumnType(Type.DATETIMEV2);
            paimonColumnType.setPrecision(localZonedTimestampType.getPrecision());
            return paimonColumnType;
        }

        @Override
        public PaimonColumnType visit(ArrayType arrayType) {
            PaimonColumnType paimonColumnType = new PaimonColumnType(Type.ARRAY);
            ColumnType elementColumnType = fromPaimonType("element", arrayType.getElementType());
            buildPaimonColumnType(Collections.singletonList(elementColumnType), paimonColumnType);
            return paimonColumnType;
        }

        @Override
        public PaimonColumnType visit(MultisetType multisetType) {
            return this.defaultMethod(multisetType);
        }

        @Override
        public PaimonColumnType visit(MapType mapType) {
            PaimonColumnType paimonColumnType = new PaimonColumnType(Type.MAP);
            ColumnType key = fromPaimonType("key", mapType.getKeyType());
            ColumnType value = fromPaimonType("value", mapType.getValueType());
            buildPaimonColumnType(Arrays.asList(key, value), paimonColumnType);
            return paimonColumnType;
        }

        @Override
        public PaimonColumnType visit(RowType rowType) {
            return this.defaultMethod(rowType);
        }

        @Override
        protected PaimonColumnType defaultMethod(DataType dataType) {
            LOG.info("UNSUPPORTED type:" + dataType);
            return new PaimonColumnType(Type.UNSUPPORTED);
        }

        private void buildPaimonColumnType(List<ColumnType> columnTypes, PaimonColumnType paimonColumnType) {
            List<PaimonColumnType> paimonColumnTypes = Lists.newArrayList();
            for (ColumnType columnType : columnTypes) {
                PaimonColumnType paimonType = new PaimonColumnType(columnType.getType());
                if (CollectionUtils.isNotEmpty(columnType.getChildTypes())) {
                    buildPaimonColumnType(columnType.getChildTypes(), paimonType);
                }
                paimonColumnTypes.add(paimonType);
            }
            paimonColumnType.setChildTypes(paimonColumnTypes);
        }
    }

    private static class PaimonColumnType {
        private Type type;
        // only used in char & varchar
        private int length;
        private int precision;
        private int scale;
        private List<PaimonColumnType> childTypes;

        public PaimonColumnType(Type type) {
            this.type = type;
            this.length = -1;
            this.precision = -1;
            this.scale = -1;
        }

        public PaimonColumnType(Type type, int length) {
            this.type = type;
            this.length = length;
            this.precision = -1;
            this.scale = -1;
        }

        public PaimonColumnType(Type type, int precision, int scale) {
            this.type = type;
            this.precision = precision;
            this.scale = scale;
            this.length = -1;
        }

        public Type getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public void setChildTypes(List<PaimonColumnType> childTypes) {
            this.childTypes = childTypes;
        }

        public List<PaimonColumnType> getChildTypes() {
            return childTypes;
        }
    }
}
