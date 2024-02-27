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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Strings;

import java.util.List;
import java.util.regex.Matcher;

public class FileFormatUtils {

    public static boolean isCsv(String formatStr) {
        return FileFormatConstants.FORMAT_CSV.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_CSV_WITH_NAMES.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES.equalsIgnoreCase(formatStr)
                || FileFormatConstants.FORMAT_HIVE_TEXT.equalsIgnoreCase(formatStr);
    }

    // public for unit test
    public static void parseCsvSchema(List<Column> csvSchema, String csvSchemaStr)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(csvSchemaStr)) {
            return;
        }
        // the schema str is like: "k1:int;k2:bigint;k3:varchar(20);k4:datetime(6)"
        String[] schemaStrs = csvSchemaStr.split(";");
        try {
            for (String schemaStr : schemaStrs) {
                String[] kv = schemaStr.replace(" ", "").split(":");
                if (kv.length != 2) {
                    throw new AnalysisException("invalid csv schema: " + csvSchemaStr);
                }
                Column column = null;
                String name = kv[0].toLowerCase();
                FeNameFormat.checkColumnName(name);
                String type = kv[1].toLowerCase();
                if (type.equals("tinyint")) {
                    column = new Column(name, PrimitiveType.TINYINT, true);
                } else if (type.equals("smallint")) {
                    column = new Column(name, PrimitiveType.SMALLINT, true);
                } else if (type.equals("int")) {
                    column = new Column(name, PrimitiveType.INT, true);
                } else if (type.equals("bigint")) {
                    column = new Column(name, PrimitiveType.BIGINT, true);
                } else if (type.equals("largeint")) {
                    column = new Column(name, PrimitiveType.LARGEINT, true);
                } else if (type.equals("float")) {
                    column = new Column(name, PrimitiveType.FLOAT, true);
                } else if (type.equals("double")) {
                    column = new Column(name, PrimitiveType.DOUBLE, true);
                } else if (type.startsWith("decimal")) {
                    // regex decimal(p, s)
                    Matcher matcher = FileFormatConstants.DECIMAL_TYPE_PATTERN.matcher(type);
                    if (!matcher.find()) {
                        throw new AnalysisException("invalid decimal type: " + type);
                    }
                    int precision = Integer.parseInt(matcher.group(1));
                    int scale = Integer.parseInt(matcher.group(2));
                    column = new Column(name, ScalarType.createDecimalV3Type(precision, scale), false, null, true, null,
                            "");
                } else if (type.equals("date")) {
                    column = new Column(name, ScalarType.createDateType(), false, null, true, null, "");
                } else if (type.startsWith("datetime")) {
                    int scale = 0;
                    if (!type.equals("datetime")) {
                        // regex datetime(s)
                        Matcher matcher = FileFormatConstants.DATETIME_TYPE_PATTERN.matcher(type);
                        if (!matcher.find()) {
                            throw new AnalysisException("invalid datetime type: " + type);
                        }
                        scale = Integer.parseInt(matcher.group(1));
                    }
                    column = new Column(name, ScalarType.createDatetimeV2Type(scale), false, null, true, null, "");
                } else if (type.equals("string")) {
                    column = new Column(name, PrimitiveType.STRING, true);
                } else if (type.equals("boolean")) {
                    column = new Column(name, PrimitiveType.BOOLEAN, true);
                } else {
                    throw new AnalysisException("unsupported column type: " + type);
                }
                csvSchema.add(column);
            }
        } catch (Exception e) {
            throw new AnalysisException("invalid csv schema: " + e.getMessage());
        }
    }
}
