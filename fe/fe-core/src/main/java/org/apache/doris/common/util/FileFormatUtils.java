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

package org.apache.doris.common.util;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class FileFormatUtils {

    public static boolean isCsv(String formatStr) {
        return FileFormatConstants.FORMAT_CSV.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_CSV_WITH_NAMES.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_HIVE_TEXT.equalsIgnoreCase(formatStr);
    }

    public static boolean isHiveFormat(String formatStr) {
        return FileFormatConstants.FORMAT_RC_BINARY.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_RC_TEXT.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_SEQUENCE.equalsIgnoreCase(formatStr);
    }

    // public for unit test
    public static void parseCsvSchema(List<Column> csvSchema, String csvSchemaStr)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(csvSchemaStr)) {
            return;
        }
        // the schema str is like: "k1:int;k2:bigint;k3:varchar(20);k4:datetime(6);" +
        // "k5:array<string>;k6:map<string,int>,k7:struct<name:string,age:int>"
        String[] schemaStrs = csvSchemaStr.split(";");
        try {
            for (String schemaStr : schemaStrs) {
                schemaStr = schemaStr.replace(" ", "");
                int colonIndex = schemaStr.indexOf(":");
                if (colonIndex == -1) {
                    throw new AnalysisException("invalid schema: " + csvSchemaStr);
                }
                String name = schemaStr.substring(0, colonIndex).toLowerCase();
                String type = schemaStr.substring(colonIndex + 1).toLowerCase();
                FeNameFormat.checkColumnName(name);

                Type columnType = parseType(type);
                Column column = new Column(name, columnType, false, null, true, null, "");

                csvSchema.add(column);
            }
        } catch (Exception e) {
            throw new AnalysisException("invalid schema: " + e.getMessage());
        }
    }

    private static Type parseType(String typeStr) throws AnalysisException {
        typeStr = typeStr.trim().toLowerCase();
        if (typeStr.equals("tinyint")) {
            return ScalarType.TINYINT;
        } else if (typeStr.equals("smallint")) {
            return ScalarType.SMALLINT;
        } else if (typeStr.equals("int")) {
            return ScalarType.INT;
        } else if (typeStr.equals("bigint")) {
            return ScalarType.BIGINT;
        } else if (typeStr.equals("largeint")) {
            return ScalarType.LARGEINT;
        } else if (typeStr.equals("float")) {
            return ScalarType.FLOAT;
        } else if (typeStr.equals("double")) {
            return ScalarType.DOUBLE;
        } else if (typeStr.startsWith("decimal")) {
            // Parse decimal(p, s)
            Matcher matcher = FileFormatConstants.DECIMAL_TYPE_PATTERN.matcher(typeStr);
            if (!matcher.find()) {
                throw new AnalysisException("Invalid decimal type: " + typeStr);
            }
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            return ScalarType.createDecimalV3Type(precision, scale);
        } else if (typeStr.equals("date")) {
            return ScalarType.createDateType();
        } else if (typeStr.startsWith("timestamp")) {
            int scale = 0;
            if (!typeStr.equals("timestamp")) {
                // Parse timestamp(s)
                Matcher matcher = FileFormatConstants.TIMESTAMP_TYPE_PATTERN.matcher(typeStr);
                if (!matcher.find()) {
                    throw new AnalysisException("Invalid timestamp type: " + typeStr);
                }
                scale = Integer.parseInt(matcher.group(1));
            }
            return ScalarType.createDatetimeV2Type(scale);
        } else if (typeStr.startsWith("datetime")) {
            int scale = 0;
            if (!typeStr.equals("datetime")) {
                // Parse datetime(s)
                Matcher matcher = FileFormatConstants.DATETIME_TYPE_PATTERN.matcher(typeStr);
                if (!matcher.find()) {
                    throw new AnalysisException("Invalid datetime type: " + typeStr);
                }
                scale = Integer.parseInt(matcher.group(1));
            }
            return ScalarType.createDatetimeV2Type(scale);
        } else if (typeStr.equals("string")) {
            return ScalarType.createStringType();
        } else if (typeStr.equals("boolean")) {
            return ScalarType.BOOLEAN;
        } else if (typeStr.startsWith("char")) {
            // Parse char(len)
            Matcher matcher = FileFormatConstants.CHAR_TYPE_PATTERN.matcher(typeStr);
            if (matcher.matches()) {
                int len = Integer.parseInt(matcher.group(1));
                return ScalarType.createChar(len);
            }
            throw new AnalysisException("Invalid char type: " + typeStr);
        } else if (typeStr.startsWith("varchar")) {
            // Parse varchar(len)
            Matcher matcher = FileFormatConstants.VARCHAR_TYPE_PATTERN.matcher(typeStr);
            if (matcher.matches()) {
                int len = Integer.parseInt(matcher.group(1));
                return ScalarType.createVarcharType(len);
            }
            throw new AnalysisException("Invalid varchar type: " + typeStr);
        } else if (typeStr.startsWith("array")) {
            // Parse array<element_type>
            if (typeStr.indexOf('<') == 5 && typeStr.endsWith(">")) {
                String elementTypeStr = typeStr.substring(6, typeStr.length() - 1);
                Type elementType = parseType(elementTypeStr);
                return new ArrayType(elementType);
            }
            throw new AnalysisException("Invalid array type: " + typeStr);
        } else if (typeStr.startsWith("map")) {
            // Parse map<key_type,value_type>
            if (typeStr.indexOf('<') == 3 && typeStr.endsWith(">")) {
                String keyValueStr = typeStr.substring(4, typeStr.length() - 1);
                int commaIndex = findCommaOutsideBrackets(keyValueStr);
                if (commaIndex == -1) {
                    throw new AnalysisException("Invalid map type: " + typeStr);
                }
                String keyTypeStr = keyValueStr.substring(0, commaIndex).trim();
                String valueTypeStr = keyValueStr.substring(commaIndex + 1).trim();
                Type keyType = parseType(keyTypeStr);
                Type valueType = parseType(valueTypeStr);
                return new MapType(keyType, valueType);
            }
            throw new AnalysisException("Invalid map type: " + typeStr);
        } else if (typeStr.startsWith("struct")) {
            // Parse struct<field1:type1,field2:type2,...>
            if (typeStr.indexOf('<') == 6 && typeStr.endsWith(">")) {
                String fieldStr = typeStr.substring(7, typeStr.length() - 1);
                List<String> fieldDefs = splitStructFields(fieldStr);
                ArrayList<StructField> structFields = new ArrayList<>();
                for (String fieldDef : fieldDefs) {
                    int colonIndex = fieldDef.indexOf(":");
                    if (colonIndex == -1) {
                        throw new AnalysisException("Invalid struct field: " + fieldDef);
                    }
                    String fieldName = fieldDef.substring(0, colonIndex).trim();
                    String fieldTypeStr = fieldDef.substring(colonIndex + 1).trim();
                    Type fieldType = parseType(fieldTypeStr);
                    StructField structField = new StructField(fieldName, fieldType);
                    structFields.add(structField);
                }
                return new StructType(structFields);
            }
            throw new AnalysisException("Invalid struct type: " + typeStr);
        } else {
            throw new AnalysisException("Unsupported type: " + typeStr);
        }
    }

    private static int findCommaOutsideBrackets(String s) {
        int level = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') {
                level++;
            } else if (c == '>') {
                level--;
            } else if (c == ',' && level == 0) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> splitStructFields(String s) throws AnalysisException {
        List<String> fields = new ArrayList<>();
        int level = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '<') {
                level++;
            } else if (c == '>') {
                level--;
            } else if (c == ',' && level == 0) {
                fields.add(s.substring(start, i).trim());
                start = i + 1;
            }
        }
        if (start < s.length()) {
            fields.add(s.substring(start).trim());
        }
        if (level != 0) {
            throw new AnalysisException("Unmatched angle brackets in struct definition.");
        }
        return fields;
    }
}
