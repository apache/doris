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

package org.apache.flink.cdc.connectors.mysql.utils;

import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;

/**
 * Copied from:
 * https://github.com/apache/flink-cdc/blob/release-3.1.1/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/utils/MySqlTypeUtils.java
 *
 * */
public class MySqlTypeUtils {

    // ------ MySQL Type ------
    // https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    private static final String BIT = "BIT";
    /*
     * BOOLEAN type will be returned when handling the change event from SQL like:
     * ALTER TABLE `student` CHANGE COLUMN `is_male` `is_female` BOOLEAN NULL;
     */
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String INTEGER = "INTEGER";
    private static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
        "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String POINT = "POINT";
    private static final String LINESTRING = "LINESTRING";
    private static final String POLYGON = "POLYGON";
    private static final String GEOMCOLLECTION = "GEOMCOLLECTION";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    private static final String MULTIPOINT = "MULTIPOINT";
    private static final String MULTIPOLYGON = "MULTIPOLYGON";
    private static final String MULTILINESTRING = "MULTILINESTRING";
    private static final String UNKNOWN = "UNKNOWN";

    /** Returns a corresponding Flink data type from a debezium {@link Column}. */
    public static DataType fromDbzColumn(Column column) {
        DataType dataType = convertFromColumn(column);
        if (column.isOptional()) {
            return dataType;
        } else {
            return dataType.notNull();
        }
    }

    /**
     * Returns a corresponding Flink data type from a debezium {@link Column} with nullable always
     * be true.
     */
    private static DataType convertFromColumn(Column column) {
        String typeName = column.typeName();
        switch (typeName) {
            case BIT:
                return column.length() == 1
                    ? DataTypes.BOOLEAN()
                    : DataTypes.BINARY((column.length() + 7) / 8);
            case BOOL:
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                // MySQL haven't boolean type, it uses tinyint(1) to represents boolean type
                // user should not use tinyint(1) to store number although jdbc url parameter
                // tinyInt1isBit=false can help change the return value, it's not a general way
                // btw: mybatis and mysql-connector-java map tinyint(1) to boolean by default
                return column.length() == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case INTEGER:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case YEAR:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED:
            case INTEGER_UNSIGNED_ZEROFILL:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return DataTypes.DECIMAL(20, 0);
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return DataTypes.FLOAT();
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return DataTypes.DOUBLE();
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return column.length() <= 38
                    ? DataTypes.DECIMAL(column.length(), column.scale().orElse(0))
                    : DataTypes.STRING();
            case TIME:
                return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return column.length() >= 0
                    ? DataTypes.TIMESTAMP(column.length())
                    : DataTypes.TIMESTAMP(0);
            case TIMESTAMP:
                return column.length() >= 0
                    ? DataTypes.TIMESTAMP_LTZ(column.length())
                    : DataTypes.TIMESTAMP_LTZ(0);
            case CHAR:
                return column.length() > 0
                    ? DataTypes.CHAR(column.length())
                    : column.length() == 0 ? CharType.ofEmptyLiteral() : DataTypes.CHAR(1);
            case VARCHAR:
                return DataTypes.VARCHAR(column.length());
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case GEOMETRYCOLLECTION:
            case GEOMCOLLECTION:
            case MULTIPOINT:
            case MULTIPOLYGON:
            case MULTILINESTRING:
                return DataTypes.STRING();
            case BINARY:
                return column.length() > 0
                    ? DataTypes.BINARY(column.length())
                    : column.length() == 0 ? BinaryType.ofEmptyLiteral() : DataTypes.BINARY(1);
            case VARBINARY:
                return DataTypes.VARBINARY(column.length());
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return DataTypes.BYTES();
            case SET:
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                    String.format("Don't support MySQL type '%s' yet.", typeName));
        }
    }

    private MySqlTypeUtils() {}
}
