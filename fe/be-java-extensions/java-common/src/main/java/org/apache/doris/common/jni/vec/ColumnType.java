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

package org.apache.doris.common.jni.vec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Column type for fields in vector table. Support complex nested types.
 * date & datetime is deprecated, use datev2 & datetimev2 only.
 * If decimalv2 is deprecated, we can unify decimal32 & decimal64 & decimal128 into decimal.
 */
public class ColumnType {
    public static final int MAX_DECIMAL32_PRECISION = 9;
    public static final int MAX_DECIMAL64_PRECISION = 18;
    public static final int MAX_DECIMAL128_PRECISION = 38;

    public enum Type {
        UNSUPPORTED(-1),
        BYTE(1), // only for string, generated as array<byte>
        BOOLEAN(1),
        TINYINT(1),
        SMALLINT(2),
        INT(4),
        BIGINT(8),
        LARGEINT(16),
        FLOAT(4),
        DOUBLE(8),
        DATEV2(4),
        DATETIMEV2(8),
        CHAR(-1),
        VARCHAR(-1),
        BINARY(-1),
        DECIMALV2(16),
        DECIMAL32(4),
        DECIMAL64(8),
        DECIMAL128(16),
        STRING(-1),
        ARRAY(-1),
        MAP(-1),
        STRUCT(-1);

        public final int size;

        Type(int size) {
            this.size = size;
        }
    }

    private final Type type;
    private final String name;
    private List<String> childNames;
    private List<ColumnType> childTypes;
    private List<Integer> fieldIndex;
    // only used in char & varchar
    private final int length;
    // only used in decimal
    private final int precision;
    private final int scale;

    public ColumnType(String name, Type type) {
        this.name = name;
        this.type = type;
        this.length = -1;
        this.precision = -1;
        this.scale = -1;
    }

    public ColumnType(String name, Type type, int length) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.precision = -1;
        this.scale = -1;
    }

    public ColumnType(String name, Type type, int precision, int scale) {
        this.name = name;
        this.type = type;
        this.length = -1;
        this.precision = precision;
        this.scale = scale;
    }

    public ColumnType(String name, Type type, int length, int precision, int scale) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
    }

    public List<String> getChildNames() {
        return childNames;
    }

    public void setChildNames(List<String> childNames) {
        this.childNames = childNames;
    }

    public List<ColumnType> getChildTypes() {
        return childTypes;
    }

    public void setChildTypes(List<ColumnType> childTypes) {
        this.childTypes = childTypes;
    }

    public List<Integer> getFieldIndex() {
        return fieldIndex;
    }

    public void setFieldIndex(List<Integer> fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    public int getTypeSize() {
        return type.size;
    }

    public boolean isUnsupported() {
        return type == Type.UNSUPPORTED;
    }

    public boolean isStringType() {
        return type == Type.STRING || type == Type.BINARY || type == Type.CHAR || type == Type.VARCHAR;
    }

    public boolean isComplexType() {
        return type == Type.ARRAY || type == Type.MAP || type == Type.STRUCT;
    }

    public boolean isArray() {
        return type == Type.ARRAY;
    }

    public boolean isMap() {
        return type == Type.MAP;
    }

    public boolean isStruct() {
        return type == Type.STRUCT;
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        return name;
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

    public int metaSize() {
        switch (type) {
            case UNSUPPORTED:
                // set nullMap address as 0.
                return 1;
            case ARRAY:
            case MAP:
            case STRUCT:
                // array & map : [nullMap | offsets | ... ]
                // struct : [nullMap | ... ]
                int size = 2;
                if (type == Type.STRUCT) {
                    size = 1;
                }
                for (ColumnType c : childTypes) {
                    size += c.metaSize();
                }
                return size;
            case STRING:
            case BINARY:
            case CHAR:
            case VARCHAR:
                // [nullMap | offsets | data ]
                return 3;
            default:
                // [nullMap | data]
                return 2;
        }
    }

    private static final Pattern digitPattern = Pattern.compile("(\\d+)");

    private static int findNextNestedField(String commaSplitFields) {
        int numLess = 0;
        int numBracket = 0;
        for (int i = 0; i < commaSplitFields.length(); i++) {
            char c = commaSplitFields.charAt(i);
            if (c == '<') {
                numLess++;
            } else if (c == '>') {
                numLess--;
            } else if (c == '(') {
                numBracket++;
            } else if (c == ')') {
                numBracket--;
            } else if (c == ',' && numLess == 0 && numBracket == 0) {
                return i;
            }
        }
        return commaSplitFields.length();
    }

    public static ColumnType parseType(String columnName, String hiveType) {
        String lowerCaseType = hiveType.toLowerCase();
        Type type = Type.UNSUPPORTED;
        int length = -1;
        int precision = -1;
        int scale = -1;
        switch (lowerCaseType) {
            case "boolean":
                type = Type.BOOLEAN;
                break;
            case "tinyint":
                type = Type.TINYINT;
                break;
            case "smallint":
                type = Type.SMALLINT;
                break;
            case "int":
                type = Type.INT;
                break;
            case "bigint":
                type = Type.BIGINT;
                break;
            case "largeint":
                type = Type.LARGEINT;
                break;
            case "float":
                type = Type.FLOAT;
                break;
            case "double":
                type = Type.DOUBLE;
                break;
            case "date":
                type = Type.DATEV2;
                break;
            case "binary":
            case "bytes":
                type = Type.BINARY;
                break;
            case "string":
                type = Type.STRING;
                break;
            default:
                if (lowerCaseType.startsWith("timestamp")) {
                    type = Type.DATETIMEV2;
                    precision = 6; // default
                    Matcher match = digitPattern.matcher(lowerCaseType);
                    if (match.find()) {
                        precision = Integer.parseInt(match.group(1).trim());
                    }
                } else if (lowerCaseType.startsWith("char")) {
                    Matcher match = digitPattern.matcher(lowerCaseType);
                    if (match.find()) {
                        type = Type.CHAR;
                        length = Integer.parseInt(match.group(1).trim());
                    }
                } else if (lowerCaseType.startsWith("varchar")) {
                    Matcher match = digitPattern.matcher(lowerCaseType);
                    if (match.find()) {
                        type = Type.VARCHAR;
                        length = Integer.parseInt(match.group(1).trim());
                    }
                } else if (lowerCaseType.startsWith("decimal")) {
                    int s = lowerCaseType.indexOf('(');
                    int e = lowerCaseType.indexOf(')');
                    if (s != -1 && e != -1) {
                        String[] ps = lowerCaseType.substring(s + 1, e).split(",");
                        precision = Integer.parseInt(ps[0].trim());
                        scale = Integer.parseInt(ps[1].trim());
                        if (lowerCaseType.startsWith("decimalv2")) {
                            type = Type.DECIMALV2;
                        } else if (lowerCaseType.startsWith("decimal32")) {
                            type = Type.DECIMAL32;
                        } else if (lowerCaseType.startsWith("decimal64")) {
                            type = Type.DECIMAL64;
                        } else if (lowerCaseType.startsWith("decimal128")) {
                            type = Type.DECIMAL128;
                        } else {
                            if (precision <= MAX_DECIMAL32_PRECISION) {
                                type = Type.DECIMAL32;
                            } else if (precision <= MAX_DECIMAL64_PRECISION) {
                                type = Type.DECIMAL64;
                            } else {
                                type = Type.DECIMAL128;
                            }
                        }
                    }
                } else if (lowerCaseType.startsWith("array")) {
                    if (lowerCaseType.indexOf("<") == 5
                            && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                        ColumnType nestedType = parseType("element",
                                lowerCaseType.substring(6, lowerCaseType.length() - 1));
                        ColumnType arrayType = new ColumnType(columnName, Type.ARRAY);
                        arrayType.setChildTypes(Collections.singletonList(nestedType));
                        return arrayType;
                    }
                } else if (lowerCaseType.startsWith("map")) {
                    if (lowerCaseType.indexOf("<") == 3
                            && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                        String keyValue = lowerCaseType.substring(4, lowerCaseType.length() - 1);
                        int index = findNextNestedField(keyValue);
                        if (index != keyValue.length() && index != 0) {
                            ColumnType keyType = parseType("key", keyValue.substring(0, index));
                            ColumnType valueType = parseType("value", keyValue.substring(index + 1));
                            ColumnType mapType = new ColumnType(columnName, Type.MAP);
                            mapType.setChildTypes(Arrays.asList(keyType, valueType));
                            return mapType;
                        }
                    }
                } else if (lowerCaseType.startsWith("struct")) {
                    if (lowerCaseType.indexOf("<") == 6
                            && lowerCaseType.lastIndexOf(">") == lowerCaseType.length() - 1) {
                        String listFields = lowerCaseType.substring(7, lowerCaseType.length() - 1);
                        ArrayList<ColumnType> fields = new ArrayList<>();
                        ArrayList<String> names = new ArrayList<>();
                        while (listFields.length() > 0) {
                            int index = findNextNestedField(listFields);
                            int pivot = listFields.indexOf(':');
                            if (pivot > 0 && pivot < listFields.length() - 1) {
                                fields.add(parseType(listFields.substring(0, pivot),
                                        listFields.substring(pivot + 1, index)));
                                names.add(listFields.substring(0, pivot));
                                listFields = listFields.substring(Math.min(index + 1, listFields.length()));
                            } else {
                                break;
                            }
                        }
                        if (listFields.isEmpty()) {
                            ColumnType structType = new ColumnType(columnName, Type.STRUCT);
                            structType.setChildTypes(fields);
                            structType.setChildNames(names);
                            return structType;
                        }
                    }
                }
                break;
        }
        return new ColumnType(columnName, type, length, precision, scale);
    }
}
