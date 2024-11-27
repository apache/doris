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

package org.apache.doris.common.jni.utils;

import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.collect.Sets;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

// Data types that are supported as return or argument types in Java UDFs.
public class JavaUdfDataType {
    public static final Logger LOG = Logger.getLogger(JavaUdfDataType.class);
    public static final JavaUdfDataType INVALID_TYPE = new JavaUdfDataType("INVALID_TYPE",
            TPrimitiveType.INVALID_TYPE, 0);
    public static final JavaUdfDataType BOOLEAN = new JavaUdfDataType("BOOLEAN", TPrimitiveType.BOOLEAN, 1);
    public static final JavaUdfDataType TINYINT = new JavaUdfDataType("TINYINT", TPrimitiveType.TINYINT, 1);
    public static final JavaUdfDataType SMALLINT = new JavaUdfDataType("SMALLINT", TPrimitiveType.SMALLINT, 2);
    public static final JavaUdfDataType INT = new JavaUdfDataType("INT", TPrimitiveType.INT, 4);
    public static final JavaUdfDataType BIGINT = new JavaUdfDataType("BIGINT", TPrimitiveType.BIGINT, 8);
    public static final JavaUdfDataType FLOAT = new JavaUdfDataType("FLOAT", TPrimitiveType.FLOAT, 4);
    public static final JavaUdfDataType DOUBLE = new JavaUdfDataType("DOUBLE", TPrimitiveType.DOUBLE, 8);
    public static final JavaUdfDataType STRING = new JavaUdfDataType("STRING", TPrimitiveType.STRING, 0);
    public static final JavaUdfDataType DATE = new JavaUdfDataType("DATE", TPrimitiveType.DATE, 8);
    public static final JavaUdfDataType DATETIME = new JavaUdfDataType("DATETIME", TPrimitiveType.DATETIME, 8);
    public static final JavaUdfDataType LARGEINT = new JavaUdfDataType("LARGEINT", TPrimitiveType.LARGEINT, 16);
    public static final JavaUdfDataType DECIMALV2 = new JavaUdfDataType("DECIMALV2", TPrimitiveType.DECIMALV2, 16);
    public static final JavaUdfDataType DATEV2 = new JavaUdfDataType("DATEV2", TPrimitiveType.DATEV2, 4);
    public static final JavaUdfDataType DATETIMEV2 = new JavaUdfDataType("DATETIMEV2", TPrimitiveType.DATETIMEV2,
            8);
    public static final JavaUdfDataType DECIMAL32 = new JavaUdfDataType("DECIMAL32", TPrimitiveType.DECIMAL32, 4);
    public static final JavaUdfDataType DECIMAL64 = new JavaUdfDataType("DECIMAL64", TPrimitiveType.DECIMAL64, 8);
    public static final JavaUdfDataType DECIMAL128 = new JavaUdfDataType("DECIMAL128", TPrimitiveType.DECIMAL128I,
            16);
    public static final JavaUdfDataType ARRAY_TYPE = new JavaUdfDataType("ARRAY_TYPE", TPrimitiveType.ARRAY, 0);
    public static final JavaUdfDataType MAP_TYPE = new JavaUdfDataType("MAP_TYPE", TPrimitiveType.MAP, 0);
    public static final JavaUdfDataType STRUCT_TYPE = new JavaUdfDataType("STRUCT_TYPE", TPrimitiveType.STRUCT, 0);

    private static Set<JavaUdfDataType> JavaUdfDataTypeSet = new HashSet<>();

    static {
        JavaUdfDataTypeSet.add(INVALID_TYPE);
        JavaUdfDataTypeSet.add(BOOLEAN);
        JavaUdfDataTypeSet.add(TINYINT);
        JavaUdfDataTypeSet.add(SMALLINT);
        JavaUdfDataTypeSet.add(INT);
        JavaUdfDataTypeSet.add(BIGINT);
        JavaUdfDataTypeSet.add(FLOAT);
        JavaUdfDataTypeSet.add(DOUBLE);
        JavaUdfDataTypeSet.add(STRING);
        JavaUdfDataTypeSet.add(DATE);
        JavaUdfDataTypeSet.add(DATETIME);
        JavaUdfDataTypeSet.add(LARGEINT);
        JavaUdfDataTypeSet.add(DECIMALV2);
        JavaUdfDataTypeSet.add(DATEV2);
        JavaUdfDataTypeSet.add(DATETIMEV2);
        JavaUdfDataTypeSet.add(DECIMAL32);
        JavaUdfDataTypeSet.add(DECIMAL64);
        JavaUdfDataTypeSet.add(DECIMAL128);
        JavaUdfDataTypeSet.add(ARRAY_TYPE);
        JavaUdfDataTypeSet.add(MAP_TYPE);
        JavaUdfDataTypeSet.add(STRUCT_TYPE);
    }

    private final String description;
    private final TPrimitiveType thriftType;
    private final int len;
    private int precision;
    private int scale;
    private Type itemType = null;
    private Type keyType;
    private Type valueType;
    private int keyScale;
    private int valueScale;
    private ArrayList<StructField> fields = new ArrayList<>();

    public JavaUdfDataType(String description, TPrimitiveType thriftType, int len) {
        this.description = description;
        this.thriftType = thriftType;
        this.len = len;
    }

    public JavaUdfDataType(JavaUdfDataType other) {
        this.description = other.description;
        this.thriftType = other.thriftType;
        this.len = other.len;
    }

    @Override
    public String toString() {
        return description;
    }

    public TPrimitiveType getPrimitiveType() {
        return thriftType;
    }

    public int getLen() {
        return len;
    }

    public static Set<JavaUdfDataType> getCandidateTypes(Class<?> c) {
        if (c == boolean.class || c == Boolean.class) {
            return Sets.newHashSet(JavaUdfDataType.BOOLEAN);
        } else if (c == byte.class || c == Byte.class) {
            return Sets.newHashSet(JavaUdfDataType.TINYINT);
        } else if (c == short.class || c == Short.class) {
            return Sets.newHashSet(JavaUdfDataType.SMALLINT);
        } else if (c == int.class || c == Integer.class) {
            return Sets.newHashSet(JavaUdfDataType.INT);
        } else if (c == long.class || c == Long.class) {
            return Sets.newHashSet(JavaUdfDataType.BIGINT);
        } else if (c == float.class || c == Float.class) {
            return Sets.newHashSet(JavaUdfDataType.FLOAT);
        } else if (c == double.class || c == Double.class) {
            return Sets.newHashSet(JavaUdfDataType.DOUBLE);
        } else if (c == char.class || c == Character.class) {
            // some users case have create UDF use varchar as parameter not
            // string type, but evaluate is String Class, so set TPrimitiveType is STRING
            return Sets.newHashSet(JavaUdfDataType.STRING);
        } else if (c == String.class) {
            return Sets.newHashSet(JavaUdfDataType.STRING);
        } else if (Type.DATE_SUPPORTED_JAVA_TYPE.contains(c)) {
            return Sets.newHashSet(JavaUdfDataType.DATE, JavaUdfDataType.DATEV2);
        } else if (Type.DATETIME_SUPPORTED_JAVA_TYPE.contains(c)) {
            return Sets.newHashSet(JavaUdfDataType.DATETIME, JavaUdfDataType.DATETIMEV2);
        } else if (c == BigInteger.class) {
            return Sets.newHashSet(JavaUdfDataType.LARGEINT);
        } else if (c == BigDecimal.class) {
            return Sets.newHashSet(JavaUdfDataType.DECIMALV2, JavaUdfDataType.DECIMAL32, JavaUdfDataType.DECIMAL64,
                    JavaUdfDataType.DECIMAL128);
        } else if (c == java.util.ArrayList.class) {
            return Sets.newHashSet(JavaUdfDataType.ARRAY_TYPE, JavaUdfDataType.STRUCT_TYPE);
        } else if (c == java.util.HashMap.class) {
            return Sets.newHashSet(JavaUdfDataType.MAP_TYPE);
        }
        return Sets.newHashSet(JavaUdfDataType.INVALID_TYPE);
    }

    public static boolean isSupported(Type t) {
        for (JavaUdfDataType javaType : JavaUdfDataTypeSet) {
            if (javaType == JavaUdfDataType.INVALID_TYPE) {
                continue;
            }
            if (javaType.getPrimitiveType() == t.getPrimitiveType().toThrift()) {
                return true;
            }
        }
        if (t.getPrimitiveType().toThrift() == TPrimitiveType.VARCHAR
                || t.getPrimitiveType().toThrift() == TPrimitiveType.CHAR) {
            return true;
        }
        return false;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return this.thriftType == TPrimitiveType.DECIMALV2 ? 9 : scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public Type getItemType() {
        return itemType;
    }

    public void setItemType(Type type) throws InternalException {
        if (this.itemType == null) {
            this.itemType = type;
        } else {
            if (!this.itemType.matchesType(type)) {
                LOG.info("set error");
                throw new InternalException("udf type not matches origin type :" + this.itemType.toSql()
                        + " set type :" + type.toSql());
            }
        }
    }

    public Type getKeyType() {
        return keyType;
    }

    public Type getValueType() {
        return valueType;
    }

    public void setKeyType(Type type) {
        this.keyType = type;
    }

    public void setValueType(Type type) {
        this.valueType = type;
    }

    public void setKeyScale(int scale) {
        this.keyScale = scale;
    }

    public void setValueScale(int scale) {
        this.valueScale = scale;
    }

    public int getKeyScale() {
        return keyScale;
    }

    public int getValueScale() {
        return valueScale;
    }

    public void setFields(ArrayList<StructField> fields) {
        this.fields = fields;
    }

    public ArrayList<String> getFieldNames() {
        ArrayList<String> names = new ArrayList<>();
        for (StructField filed : fields) {
            names.add(filed.getName());
        }
        return names;
    }
}
