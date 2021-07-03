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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetPropertyAnalyzer {

    private static final String PARQUET_PROP_PREFIX = "parquet.";
    private static final List<String> PARQUET_REPETITION_TYPES = Lists.newArrayList();
    private static final List<String> PARQUET_DATA_TYPES = Lists.newArrayList();
    private static final int REPETITION_TYPE_INDEX = 0;
    private static final int DATA_TYPE_INDEX = 1;
    private static final int COLUMN_NAME_INDEX = 2;
    private static final int SCHEMA_FIELD_COUNT = 3;

    static {
        PARQUET_REPETITION_TYPES.add("required");
        PARQUET_REPETITION_TYPES.add("repeated");
        PARQUET_REPETITION_TYPES.add("optional");

        PARQUET_DATA_TYPES.add("boolean");
        PARQUET_DATA_TYPES.add("int32");
        PARQUET_DATA_TYPES.add("int64");
        PARQUET_DATA_TYPES.add("int96");
        PARQUET_DATA_TYPES.add("byte_array");
        PARQUET_DATA_TYPES.add("float");
        PARQUET_DATA_TYPES.add("double");
        PARQUET_DATA_TYPES.add("fixed_len_byte_array");
    }

    // schema defined like the follow format:
    // "schema" = "required, int32, col_name1; required, byte_array, col_name2"
    public static List<List<String>> parseSchema(String schema) throws AnalysisException {
        List<List<String>> columns = new ArrayList<>();
        if (schema == null || schema.length() <= 0) {
            throw new AnalysisException("schema is required for parquet file");
        }
        schema = schema.replace(" ","");
        schema = schema.toLowerCase();
        String[] schemas = schema.split(";");
        for (String item:schemas) {
            String[] properties = item.split(",");
            if (properties.length != SCHEMA_FIELD_COUNT) {
                throw new AnalysisException("must only contains repetition type/data type/column name");
            }
            if (!PARQUET_REPETITION_TYPES.contains(properties[REPETITION_TYPE_INDEX])) {
                throw new AnalysisException("unknown repetition type");
            }
            if (!properties[REPETITION_TYPE_INDEX].equalsIgnoreCase("required")) {
                throw new AnalysisException("currently only support required type");
            }
            if (!PARQUET_DATA_TYPES.contains(properties[DATA_TYPE_INDEX])) {
                throw new AnalysisException("data type is not supported:"+properties[1]);
            }
            if (properties[COLUMN_NAME_INDEX] == null || properties[COLUMN_NAME_INDEX].isEmpty()) {
                throw new AnalysisException("column name can not be empty");
            }
            columns.add(Arrays.asList(properties));
        }
        return columns;
    }

    public static Map<String, String> parseFileProperties(Map<String, String> properties, Set<String> processedPropKeys) {
        Map<String, String> fileProperties = new HashMap<>();
        Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(PARQUET_PROP_PREFIX)) {
                processedPropKeys.add(entry.getKey());
                fileProperties.put(entry.getKey().substring(PARQUET_PROP_PREFIX.length()), entry.getValue());
            }
        }
        return fileProperties;
    }

    public static void checkOutExprAndSchema(List<List<String>> schema, List<Expr> outExpr) throws AnalysisException {
        // check schema number
        if (outExpr.size() != schema.size()) {
            throw new AnalysisException("Parquet schema number does not equal to select item number");
        }

        // check type
        for (int i = 0; i < schema.size(); ++i) {
            String schemaType = schema.get(i).get(DATA_TYPE_INDEX);
            PrimitiveType resultType = outExpr.get(i).getType().getPrimitiveType();
            checkSchemaAndOutExprType(schemaType, resultType, outExpr.get(i).toSql());
        }
    }

    public static void checkProjectionFieldAndSchema(List<List<String>> schema, List<Column> columns) throws AnalysisException {
        // check schema number
        if (columns.size() != schema.size()) {
            throw new AnalysisException("Parquet schema number does not equal to projection field number");
        }
        // check type
        for (int i = 0; i < schema.size(); ++i) {
            String schemaType = schema.get(i).get(DATA_TYPE_INDEX);
            PrimitiveType resultType = columns.get(i).getDataType();
            checkSchemaAndOutExprType(schemaType, resultType, columns.get(i).getName());
        }
    }

    public static void checkProjectionFieldAndSchema(List<List<String>> schema, Table table) throws AnalysisException {
        List<Column> columns = table.getBaseSchema();
        checkProjectionFieldAndSchema(schema, columns);
    }

    public static void checkSchemaAndOutExprType(String schemaType, PrimitiveType resultType, String columnName) throws AnalysisException {
        switch (resultType) {
            case BOOLEAN:
                if (!schemaType.equals("boolean")) {
                    throw new AnalysisException(columnName + " is BOOLEAN, should use boolean, " +
                            "but schema type is " + schemaType);
                }
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
                if (!schemaType.equals("int32")) {
                    throw new AnalysisException(columnName + " is TINYINT/SMALLINT/INT, should use int32, "
                            + "but schema type is " + schemaType);
                }
                break;
            case BIGINT:
            case DATE:
            case DATETIME:
                if (!schemaType.equals("int64")) {
                    throw new AnalysisException(columnName + " is BIGINT/DATE/DATETIME, should use int64, " +
                            "but schema type is " + schemaType);
                }
                break;
            case FLOAT:
                if (!schemaType.equals("float")) {
                    throw new AnalysisException(columnName + " is FLOAT, should use float," +
                            " but schema type is " + schemaType);
                }
                break;
            case DOUBLE:
                if (!schemaType.equals("double")) {
                    throw new AnalysisException(columnName + " is DOUBLE, should use double, " +
                            "but schema type is " + schemaType);
                }
                break;
            case CHAR:
            case VARCHAR:
            case DECIMAL:
            case DECIMALV2:
                if (!schemaType.equals("byte_array")) {
                    throw new AnalysisException(columnName + " is CHAR/VARCHAR/DECIMAL, should use byte_array, " +
                            "but schema type is " + schemaType);
                }
                break;
            default:
                throw new AnalysisException("Parquet format does not support column type: " + resultType);
        }
    }

    public static List<List<String>> genSchemaByResultExpr(List<Expr> resultExprs) throws AnalysisException {
        List<List<String>> schema = new ArrayList<>();
        for (int i = 0; i < resultExprs.size(); ++i) {
            Expr expr = resultExprs.get(i);
            List<String> column = new ArrayList<>();
            column.add("required");
            switch (expr.getType().getPrimitiveType()) {
                case BOOLEAN:
                    column.add("boolean");
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    column.add("int32");
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    column.add("int64");
                    break;
                case FLOAT:
                    column.add("float");
                    break;
                case DOUBLE:
                    column.add("double");
                    break;
                case CHAR:
                case VARCHAR:
                case DECIMAL:
                case DECIMALV2:
                    column.add("byte_array");
                    break;
                default:
                    throw new AnalysisException("currently parquet do not support column type: " + expr.getType().getPrimitiveType());
            }
            column.add("col" + i);
            schema.add(column);
        }
        return schema;
    }

    public static List<List<String>> genSchema(List<Column> cols) throws AnalysisException {
        List<List<String>> schema = new ArrayList<>();
        for (int i = 0; i < cols.size(); ++i) {
            Column col = cols.get(i);
            List<String> column = new ArrayList<>();
            column.add("required");
            switch (col.getDataType()) {
                case BOOLEAN:
                    column.add("boolean");
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    column.add("int32");
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    column.add("int64");
                    break;
                case FLOAT:
                    column.add("float");
                    break;
                case DOUBLE:
                    column.add("double");
                    break;
                case CHAR:
                case VARCHAR:
                case DECIMAL:
                case DECIMALV2:
                    column.add("byte_array");
                    break;
                default:
                    throw new AnalysisException("currently parquet do not support column type: " + col.getDataType());
            }
            column.add(col.getName());
            schema.add(column);
        }
        return schema;
    }

    public static List<List<String>> genSchema(Table table) throws AnalysisException {
        List<List<String>> schema = new ArrayList<>();
        table.readLock();
        try {
            List<Column> cols = table.getBaseSchema();
            schema = genSchema(cols);
        } finally {
            table.readUnlock();
        }
        return schema;
    }

    public static void compareSchemaAndResultExpr(List<Expr> resultExprs, List<List<String>> schema) throws AnalysisException {
        // check schema number
        if (resultExprs.size() != schema.size()) {
            throw new AnalysisException("Parquet schema number does not equal to select item number");
        }

        // check type
        for (int i = 0; i < schema.size(); ++i) {
            String type = schema.get(i).get(1);
            Type resultType = resultExprs.get(i).getType();
            switch (resultType.getPrimitiveType()) {
                case BOOLEAN:
                    if (!type.equals("boolean")) {
                        throw new AnalysisException("project field type is BOOLEAN, should use boolean, but the type of column "
                                + i + " is " + type);
                    }
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                    if (!type.equals("int32")) {
                        throw new AnalysisException("project field type is TINYINT/SMALLINT/INT, should use int32, "
                                + "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case BIGINT:
                case DATE:
                case DATETIME:
                    if (!type.equals("int64")) {
                        throw new AnalysisException("project field type is BIGINT/DATE/DATETIME, should use int64, " +
                                "but the definition type of column " + i + " is " + type);
                    }
                    break;
                case FLOAT:
                    if (!type.equals("float")) {
                        throw new AnalysisException("project field type is FLOAT, should use float, but the definition type of column "
                                + i + " is " + type);
                    }
                    break;
                case DOUBLE:
                    if (!type.equals("double")) {
                        throw new AnalysisException("project field type is DOUBLE, should use double, but the definition type of column "
                                + i + " is " + type);
                    }
                    break;
                case CHAR:
                case VARCHAR:
                case DECIMALV2:
                    if (!type.equals("byte_array")) {
                        throw new AnalysisException("project field type is CHAR/VARCHAR/DECIMAL, should use byte_array, " +
                                "but the definition type of column " + i + " is " + type);
                    }
                    break;
                default:
                    throw new AnalysisException("Parquet format does not support column type: " + resultType.getPrimitiveType());
            }
        }
    }
}
