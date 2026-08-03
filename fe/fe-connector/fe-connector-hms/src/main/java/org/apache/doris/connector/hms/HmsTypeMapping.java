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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps Hive type strings to {@link ConnectorType}.
 *
 * <p>This is the SPI-clean equivalent of
 * {@code HiveMetaStoreClientHelper.hiveTypeToDorisType()}.
 * It uses only connector-api types, with no fe-core dependency.</p>
 *
 * <p>Type mapping options can be customized via {@link Options}.</p>
 */
public final class HmsTypeMapping {

    private static final Pattern DIGIT_PATTERN = Pattern.compile("(\\d+)");

    public static final int DEFAULT_TIME_SCALE = 6;
    private static final int DEFAULT_DECIMAL_PRECISION = 9;
    private static final int DEFAULT_DECIMAL_SCALE = 0;

    private HmsTypeMapping() {
    }

    /**
     * Convert a Hive type string to {@link ConnectorType} with default options.
     */
    public static ConnectorType toConnectorType(String hiveType) {
        return toConnectorType(hiveType, Options.DEFAULT);
    }

    /**
     * Convert a Hive type string to {@link ConnectorType}.
     *
     * @param hiveType the Hive type string (e.g. "int", "array&lt;string&gt;",
     *                 "struct&lt;a:int,b:string&gt;")
     * @param options  type mapping options
     * @return the corresponding ConnectorType, or ConnectorType.of("UNSUPPORTED")
     */
    public static ConnectorType toConnectorType(String hiveType, Options options) {
        return toConnectorTypeInternal(hiveType.toLowerCase(), options);
    }

    private static ConnectorType toConnectorTypeInternal(String lowerType,
            Options options) {
        // Primitive types
        switch (lowerType) {
            case "boolean":
                return ConnectorType.of("BOOLEAN");
            case "tinyint":
                return ConnectorType.of("TINYINT");
            case "smallint":
                return ConnectorType.of("SMALLINT");
            case "int":
                return ConnectorType.of("INT");
            case "bigint":
                return ConnectorType.of("BIGINT");
            case "date":
                return ConnectorType.of("DATEV2");
            case "timestamp":
                return ConnectorType.of("DATETIMEV2", options.timeScale, -1);
            case "float":
                return ConnectorType.of("FLOAT");
            case "double":
                return ConnectorType.of("DOUBLE");
            case "string":
                return ConnectorType.of("STRING");
            case "binary":
                return options.mapBinaryToVarbinary
                        ? ConnectorType.of("VARBINARY")
                        : ConnectorType.of("STRING");
            default:
                break;
        }

        // ARRAY<elementType>
        if (lowerType.startsWith("array")) {
            int lt = lowerType.indexOf('<');
            int gt = lowerType.lastIndexOf('>');
            if (lt == 5 && gt == lowerType.length() - 1) {
                ConnectorType element = toConnectorTypeInternal(
                        lowerType.substring(6, gt), options);
                return ConnectorType.arrayOf(element);
            }
        }

        // MAP<keyType, valueType>
        if (lowerType.startsWith("map")) {
            int lt = lowerType.indexOf('<');
            int gt = lowerType.lastIndexOf('>');
            if (lt == 3 && gt == lowerType.length() - 1) {
                String keyValue = lowerType.substring(4, gt);
                int sep = findNextNestedField(keyValue);
                if (sep > 0 && sep < keyValue.length()) {
                    ConnectorType keyType = toConnectorTypeInternal(
                            keyValue.substring(0, sep).trim(), options);
                    ConnectorType valType = toConnectorTypeInternal(
                            keyValue.substring(sep + 1).trim(), options);
                    return ConnectorType.mapOf(keyType, valType);
                }
            }
        }

        // STRUCT<name1:type1, name2:type2, ...>
        if (lowerType.startsWith("struct")) {
            int lt = lowerType.indexOf('<');
            int gt = lowerType.lastIndexOf('>');
            if (lt == 6 && gt == lowerType.length() - 1) {
                String listFields = lowerType.substring(7, gt);
                List<String> names = new ArrayList<>();
                List<ConnectorType> types = new ArrayList<>();
                while (listFields.length() > 0) {
                    int index = findNextNestedField(listFields);
                    int pivot = listFields.indexOf(':');
                    if (pivot > 0 && pivot < listFields.length() - 1) {
                        names.add(listFields.substring(0, pivot).trim());
                        types.add(toConnectorTypeInternal(
                                listFields.substring(pivot + 1, index).trim(),
                                options));
                        listFields = listFields.substring(
                                Math.min(index + 1, listFields.length()));
                    } else {
                        break;
                    }
                }
                if (listFields.isEmpty() && !names.isEmpty()) {
                    return ConnectorType.structOf(names, types);
                }
            }
        }

        // CHAR(N)
        if (lowerType.startsWith("char")) {
            Matcher match = DIGIT_PATTERN.matcher(lowerType);
            if (match.find()) {
                int len = Integer.parseInt(match.group(1));
                return ConnectorType.of("CHAR", len, -1);
            }
            return ConnectorType.of("CHAR");
        }

        // VARCHAR(N)
        if (lowerType.startsWith("varchar")) {
            Matcher match = DIGIT_PATTERN.matcher(lowerType);
            if (match.find()) {
                int len = Integer.parseInt(match.group(1));
                return ConnectorType.of("VARCHAR", len, -1);
            }
            return ConnectorType.of("VARCHAR");
        }

        // DECIMAL(P, S)
        if (lowerType.startsWith("decimal")) {
            Matcher match = DIGIT_PATTERN.matcher(lowerType);
            int precision = DEFAULT_DECIMAL_PRECISION;
            int scale = DEFAULT_DECIMAL_SCALE;
            if (match.find()) {
                precision = Integer.parseInt(match.group(1));
            }
            if (match.find()) {
                scale = Integer.parseInt(match.group(1));
            }
            return ConnectorType.of("DECIMALV3", precision, scale);
        }

        // TIMESTAMP WITH LOCAL TIME ZONE
        if (lowerType.startsWith("timestamp with local time zone")) {
            return options.mapTimestampTz
                    ? ConnectorType.of("TIMESTAMPTZ", options.timeScale, -1)
                    : ConnectorType.of("DATETIMEV2", options.timeScale, -1);
        }

        return ConnectorType.of("UNSUPPORTED");
    }

    /**
     * Convert a {@link ConnectorType} to its Hive type string. This is the reverse direction of
     * {@link #toConnectorType(String, Options)} for the shapes a Doris CREATE TABLE emits, and the
     * SPI-clean equivalent of fe-core's {@code HiveMetaStoreClientHelper.dorisTypeToHiveType()}.
     *
     * <p>Scalar type names are Doris {@code PrimitiveType} names, matching what fe-core's
     * {@code ConnectorColumnConverter.toConnectorType} emits ({@code PrimitiveType.toString()}); complex
     * types use "ARRAY"/"MAP"/"STRUCT" with populated children. CHAR carries its length in the precision
     * field (mirroring the create-request encoding). VARCHAR and STRING both map to Hive {@code string},
     * matching legacy.</p>
     *
     * @throws IllegalArgumentException for a type Hive tables cannot represent — matching legacy, which
     *         threw for the same unsupported primitive/complex shapes.
     */
    public static String toHiveTypeString(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "ARRAY": {
                List<ConnectorType> children = type.getChildren();
                if (children.isEmpty()) {
                    throw new IllegalArgumentException("Unsupported type conversion of " + type);
                }
                return "array<" + toHiveTypeString(children.get(0)) + ">";
            }
            case "MAP": {
                List<ConnectorType> children = type.getChildren();
                if (children.size() < 2) {
                    throw new IllegalArgumentException("Unsupported type conversion of " + type);
                }
                return "map<" + toHiveTypeString(children.get(0)) + ","
                        + toHiveTypeString(children.get(1)) + ">";
            }
            case "STRUCT": {
                List<ConnectorType> children = type.getChildren();
                List<String> fieldNames = type.getFieldNames();
                StringBuilder sb = new StringBuilder("struct<");
                for (int i = 0; i < children.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
                    sb.append(fieldName).append(":").append(toHiveTypeString(children.get(i)));
                }
                sb.append(">");
                return sb.toString();
            }
            default:
                return scalarToHiveTypeString(name, type);
        }
    }

    private static String scalarToHiveTypeString(String name, ConnectorType type) {
        switch (name) {
            case "BOOLEAN":
                return "boolean";
            case "TINYINT":
                return "tinyint";
            case "SMALLINT":
                return "smallint";
            case "INT":
                return "int";
            case "BIGINT":
                return "bigint";
            case "DATE":
            case "DATEV2":
                return "date";
            case "DATETIME":
            case "DATETIMEV2":
                return "timestamp";
            case "FLOAT":
                return "float";
            case "DOUBLE":
                return "double";
            case "CHAR":
                return "char(" + type.getPrecision() + ")";
            case "VARCHAR":
            case "STRING":
                return "string";
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
            case "DECIMALV3": {
                int precision = type.getPrecision();
                if (precision == 0) {
                    precision = DEFAULT_DECIMAL_PRECISION;
                }
                return "decimal(" + precision + "," + type.getScale() + ")";
            }
            default:
                throw new IllegalArgumentException("Unsupported type conversion of " + type);
        }
    }

    /**
     * Find the index of the next top-level comma separator in a
     * comma-separated nested type string. Respects angle-bracket and
     * parenthesis nesting.
     */
    static int findNextNestedField(String fields) {
        int angleBrackets = 0;
        int parens = 0;
        for (int i = 0; i < fields.length(); i++) {
            char c = fields.charAt(i);
            if (c == '<') {
                angleBrackets++;
            } else if (c == '>') {
                angleBrackets--;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (c == ',' && angleBrackets == 0 && parens == 0) {
                return i;
            }
        }
        return fields.length();
    }

    /**
     * Options controlling type mapping behavior.
     */
    public static final class Options {

        /** Default options: timeScale=6, no varbinary mapping, no timestamptz. */
        public static final Options DEFAULT = new Options(
                DEFAULT_TIME_SCALE, false, false);

        private final int timeScale;
        private final boolean mapBinaryToVarbinary;
        private final boolean mapTimestampTz;

        public Options(int timeScale, boolean mapBinaryToVarbinary,
                boolean mapTimestampTz) {
            this.timeScale = timeScale;
            this.mapBinaryToVarbinary = mapBinaryToVarbinary;
            this.mapTimestampTz = mapTimestampTz;
        }

        public int getTimeScale() {
            return timeScale;
        }

        public boolean isMapBinaryToVarbinary() {
            return mapBinaryToVarbinary;
        }

        public boolean isMapTimestampTz() {
            return mapTimestampTz;
        }
    }
}
