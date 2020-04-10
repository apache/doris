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

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

import com.google.common.collect.Lists;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;

public class DppUtils {
    public static final String BUCKET_ID = "__bucketId__";
    public static Class dataTypeToClass(DataType dataType) {
        if (dataType == null) {
            return null;
        }
        if (dataType.equals(DataTypes.BooleanType)) {
            return Boolean.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        } else if (dataType.equals(DataTypes.IntegerType)) {
            return Integer.class;
        } else if (dataType.equals(DataTypes.LongType)) {
            return Long.class;
        } else if (dataType.equals(DataTypes.FloatType)) {
            return Float.class;
        } else if (dataType.equals(DataTypes.DoubleType)) {
            return Double.class;
        } else if (dataType.equals(DataTypes.DateType)) {
            return Date.class;
        } else if (dataType.equals(DataTypes.StringType)) {
            return String.class;
        }
        return null;
    }

    public static Class columnTypeToClass(String columnType) {
        switch (columnType) {
            case "BOOL":
                return Boolean.class;
            case "TINYINT":
            case "SMALLINT":
                return Short.class;
            case "INT":
                return Integer.class;
            case "DATETIME":
            case "BIGINT":
                return Long.class;
            case "LARGEINT":
                return BigInteger.class;
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
                return Double.class;
            case "DATE":
                return Date.class;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
                return String.class;
            default:
                return String.class;
        }
    }

    public static DataType columnTypeToDataType(String columnType) throws UserException {
        DataType dataType = DataTypes.StringType;
        switch (columnType) {
            case "BOOL":
                dataType = DataTypes.BooleanType;
                break;
            case "TINYINT":
            case "SMALLINT":
                dataType = DataTypes.ShortType;
                break;
            case "INT":
                dataType = DataTypes.IntegerType;
                break;
            case "DATETIME":
            case "BIGINT":
            case "LARGEINT":
                // todo: special treat LARGEINT because spark do not support int128
                dataType = DataTypes.LongType;
                break;
            case "FLOAT":
                dataType = DataTypes.FloatType;
                break;
            case "DOUBLE":
                dataType = DataTypes.DoubleType;
                break;
            case "DATE":
                dataType = DataTypes.DateType;
                break;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
                dataType = DataTypes.StringType;
                break;
            default:
                throw new UserException("Reason: invalid column type:" + columnType);
        }
        return dataType;
    }

    public static ByteBuffer getHashValue(Object o, DataType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (o == null) {
            buffer.putInt(0);
            return buffer;
        }
        if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short)o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long)o);
        } else if (type.equals(DataTypes.StringType)) {
            try {
                String str = (String)o;
                buffer = ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return buffer;
    }

    public static long getHashValue(Row row, List<String> distributeColumns, StructType dstTableSchema) {
        CRC32 hashValue = new CRC32();
        for (String distColumn : distributeColumns) {
            Object columnObject = row.get(row.fieldIndex(distColumn));
            ByteBuffer buffer = getHashValue(columnObject, dstTableSchema.apply(distColumn).dataType());
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

    public static StructType createDstTableSchema(List<EtlJobConfig.EtlColumn> columns, boolean addBucketIdColumn) throws UserException {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (EtlJobConfig.EtlColumn column : columns) {
            String columnName = column.columnName;
            String columnType = column.columnType;
            DataType structColumnType = columnTypeToDataType(columnType);
            StructField field = DataTypes.createStructField(columnName, structColumnType, column.isAllowNull);
            fields.add(field);
        }
        StructType dstSchema = DataTypes.createStructType(fields);
        return dstSchema;
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath) throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            int index = columnsFromPath.indexOf(pair[0]);
            if (index == -1) {
                continue;
            }
            columns[index] = pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }
}
