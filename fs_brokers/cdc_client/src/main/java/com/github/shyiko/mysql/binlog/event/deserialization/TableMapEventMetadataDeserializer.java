/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata.DefaultCharset;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Copied from Flink CDC 3.6.0-1.20 (flink-connector-mysql-cdc).
 *
 * <p>Lines 109-111 and 204: decode VECTOR_DIMENSIONALITY (field 13) as packed integers and
 * store the dimensions in TableMapEventMetadata; retain the signedness block boundary handling.
 */
public class TableMapEventMetadataDeserializer {

    private final Logger logger = Logger.getLogger(getClass().getName());

    public TableMapEventMetadata deserialize(
            ByteArrayInputStream inputStream, int nColumns, int nNumericColumns)
            throws IOException {
        int remainingBytes = inputStream.available();
        if (remainingBytes <= 0) {
            return null;
        }

        TableMapEventMetadata result = new TableMapEventMetadata();

        for (; remainingBytes > 0; inputStream.enterBlock(remainingBytes)) {
            int code = inputStream.readInteger(1);

            MetadataFieldType fieldType = MetadataFieldType.byCode(code);
            if (fieldType == null) {
                throw new IOException("Unsupported table metadata field type " + code);
            }
            if (MetadataFieldType.UNKNOWN_METADATA_FIELD_TYPE.equals(fieldType)) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Received metadata field of unknown type");
                }
                continue;
            }

            int fieldLength = inputStream.readPackedInteger();

            remainingBytes = inputStream.available();
            inputStream.enterBlock(fieldLength);

            switch (fieldType) {
                case SIGNEDNESS:
                    result.setSignedness(readBooleanList(inputStream, nNumericColumns));
                    inputStream.skipToTheEndOfTheBlock();
                    break;
                case DEFAULT_CHARSET:
                    result.setDefaultCharset(readDefaultCharset(inputStream));
                    break;
                case COLUMN_CHARSET:
                    result.setColumnCharsets(readIntegers(inputStream));
                    break;
                case COLUMN_NAME:
                    result.setColumnNames(readColumnNames(inputStream));
                    break;
                case SET_STR_VALUE:
                    result.setSetStrValues(readTypeValues(inputStream));
                    break;
                case ENUM_STR_VALUE:
                    result.setEnumStrValues(readTypeValues(inputStream));
                    break;
                case GEOMETRY_TYPE:
                    result.setGeometryTypes(readIntegers(inputStream));
                    break;
                case SIMPLE_PRIMARY_KEY:
                    result.setSimplePrimaryKeys(readIntegers(inputStream));
                    break;
                case PRIMARY_KEY_WITH_PREFIX:
                    result.setPrimaryKeysWithPrefix(readIntegerPairs(inputStream));
                    break;
                case ENUM_AND_SET_DEFAULT_CHARSET:
                    result.setEnumAndSetDefaultCharset(readDefaultCharset(inputStream));
                    break;
                case ENUM_AND_SET_COLUMN_CHARSET:
                    result.setEnumAndSetColumnCharsets(readIntegers(inputStream));
                    result.setVisibility(readBooleanList(inputStream, nColumns));
                    break;
                case VECTOR_DIMENSIONALITY:
                    result.setVectorDimensionality(readIntegers(inputStream));
                    break;
                case VISIBILITY:
                    result.setVisibility(readBooleanList(inputStream, nColumns));
                    break;
                default:
                    inputStream.enterBlock(remainingBytes);
                    throw new IOException("Unsupported table metadata field type " + code);
            }
            remainingBytes -= fieldLength;
        }
        return result;
    }

    private static BitSet readBooleanList(ByteArrayInputStream inputStream, int length)
            throws IOException {
        BitSet result = new BitSet();
        // according to MySQL internals the amount of storage required for N columns is INT((N+7)/8)
        // bytes
        byte[] bytes = inputStream.read((length + 7) >> 3);
        for (int i = 0; i < length; ++i) {
            if ((bytes[i >> 3] & (1 << (7 - (i % 8)))) != 0) {
                result.set(i);
            }
        }
        return result;
    }

    private static DefaultCharset readDefaultCharset(ByteArrayInputStream inputStream)
            throws IOException {
        TableMapEventMetadata.DefaultCharset result = new TableMapEventMetadata.DefaultCharset();
        result.setDefaultCharsetCollation(inputStream.readPackedInteger());
        Map<Integer, Integer> charsetCollations = readIntegerPairs(inputStream);
        if (!charsetCollations.isEmpty()) {
            result.setCharsetCollations(charsetCollations);
        }
        return result;
    }

    private static List<Integer> readIntegers(ByteArrayInputStream inputStream) throws IOException {
        List<Integer> result = new ArrayList<Integer>();
        while (inputStream.available() > 0) {
            result.add(inputStream.readPackedInteger());
        }
        return result;
    }

    private static List<String> readColumnNames(ByteArrayInputStream inputStream)
            throws IOException {
        List<String> columnNames = new ArrayList<String>();
        while (inputStream.available() > 0) {
            columnNames.add(inputStream.readLengthEncodedString());
        }
        return columnNames;
    }

    private static List<String[]> readTypeValues(ByteArrayInputStream inputStream)
            throws IOException {
        List<String[]> result = new ArrayList<String[]>();
        while (inputStream.available() > 0) {
            List<String> typeValues = new ArrayList<String>();
            int valuesCount = inputStream.readPackedInteger();
            for (int i = 0; i < valuesCount; ++i) {
                typeValues.add(inputStream.readLengthEncodedString());
            }
            result.add(typeValues.toArray(new String[typeValues.size()]));
        }
        return result;
    }

    private static Map<Integer, Integer> readIntegerPairs(ByteArrayInputStream inputStream)
            throws IOException {
        Map<Integer, Integer> result = new LinkedHashMap<Integer, Integer>();
        while (inputStream.available() > 0) {
            int columnIndex = inputStream.readPackedInteger();
            int columnCharset = inputStream.readPackedInteger();
            result.put(columnIndex, columnCharset);
        }
        return result;
    }

    private enum MetadataFieldType {
        SIGNEDNESS(1), // Signedness of numeric colums
        DEFAULT_CHARSET(2), // Charsets of character columns
        COLUMN_CHARSET(3), // Charsets of character columns
        COLUMN_NAME(4), // Names of columns
        SET_STR_VALUE(5), // The string values of SET columns
        ENUM_STR_VALUE(6), // The string values is ENUM columns
        GEOMETRY_TYPE(7), // The real type of geometry columns
        SIMPLE_PRIMARY_KEY(8), // The primary key without any prefix
        PRIMARY_KEY_WITH_PREFIX(9), // The primary key with some prefix
        ENUM_AND_SET_DEFAULT_CHARSET(10), // Charsets of ENUM and SET columns
        ENUM_AND_SET_COLUMN_CHARSET(11), // Charsets of ENUM and SET columns
        VISIBILITY(12), // Column visibility (8.0.23 and newer)
        VECTOR_DIMENSIONALITY(13),
        UNKNOWN_METADATA_FIELD_TYPE(
                128); // Returned with binlog-row-metadata=FULL from MySQL 8.0 in some cases

        private final int code;

        private MetadataFieldType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        private static final Map<Integer, MetadataFieldType> INDEX_BY_CODE;

        static {
            INDEX_BY_CODE = new HashMap<Integer, MetadataFieldType>();
            for (MetadataFieldType fieldType : values()) {
                INDEX_BY_CODE.put(fieldType.code, fieldType);
            }
        }

        public static MetadataFieldType byCode(int code) {
            return INDEX_BY_CODE.get(code);
        }
    }
}
