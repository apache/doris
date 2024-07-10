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

package org.apache.flink.cdc.connectors.mysql.source.parser;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

/**
 * Copied from:
 * https://github.com/apache/flink-cdc/blob/release-3.1.1/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/parser/CustomMySqlAntlrDdlParser.java
 *
 * */
public class CustomMySqlAntlrDdlParser extends MySqlAntlrDdlParser {

    private final LinkedList<SchemaChangeEvent> parsedEvents;

    public CustomMySqlAntlrDdlParser() {
        super();
        this.parsedEvents = new LinkedList<>();
    }

    // Overriding this method because the BIT type requires default length dimension of 1.
    // Remove it when debezium fixed this issue.
    @Override
    protected DataTypeResolver initializeDataTypeResolver() {
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.StringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.CHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.CHAR, MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT),
                        new DataTypeResolver.DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR),
                        new DataTypeResolver.DataTypeEntry(
                                Types.CHAR, MySqlParser.CHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.VARCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.TINYTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.TEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.MEDIUMTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.LONGTEXT, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NCHAR, MySqlParser.NCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR, MySqlParser.NVARCHAR, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.CHARACTER),
                        new DataTypeResolver.DataTypeEntry(
                                Types.VARCHAR, MySqlParser.CHARACTER, MySqlParser.VARYING)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.NationalStringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.VARCHAR)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(
                                        Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARCHAR)
                                .setSuffixTokens(MySqlParser.BINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.NationalVaryingStringDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR,
                                MySqlParser.NATIONAL,
                                MySqlParser.CHAR,
                                MySqlParser.VARYING),
                        new DataTypeResolver.DataTypeEntry(
                                Types.NVARCHAR,
                                MySqlParser.NATIONAL,
                                MySqlParser.CHARACTER,
                                MySqlParser.VARYING)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.DimensionDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.TINYINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.INT1)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.SMALLINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.SMALLINT, MySqlParser.INT2)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.MEDIUMINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT3)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.MIDDLEINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INTEGER)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.INT4)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.BIGINT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.INT8)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.REAL, MySqlParser.REAL)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MySqlParser.DOUBLE)
                                .setSuffixTokens(
                                        MySqlParser.PRECISION,
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DOUBLE, MySqlParser.FLOAT8)
                                .setSuffixTokens(
                                        MySqlParser.PRECISION,
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT4)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.DECIMAL)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.DEC)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.DECIMAL, MySqlParser.FIXED)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.NUMERIC, MySqlParser.NUMERIC)
                                .setSuffixTokens(
                                        MySqlParser.SIGNED,
                                        MySqlParser.UNSIGNED,
                                        MySqlParser.ZEROFILL)
                                .setDefaultLengthScaleDimension(10, 0),
                        new DataTypeResolver.DataTypeEntry(Types.BIT, MySqlParser.BIT)
                                .setDefaultLengthDimension(1),
                        new DataTypeResolver.DataTypeEntry(Types.TIME, MySqlParser.TIME),
                        new DataTypeResolver.DataTypeEntry(
                                Types.TIMESTAMP_WITH_TIMEZONE, MySqlParser.TIMESTAMP),
                        new DataTypeResolver.DataTypeEntry(Types.TIMESTAMP, MySqlParser.DATETIME),
                        new DataTypeResolver.DataTypeEntry(Types.BINARY, MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.VARBINARY, MySqlParser.VARBINARY),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.BLOB),
                        new DataTypeResolver.DataTypeEntry(Types.INTEGER, MySqlParser.YEAR)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.SimpleDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.DATE, MySqlParser.DATE),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.TINYBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.MEDIUMBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.LONGBLOB),
                        new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOL),
                        new DataTypeResolver.DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOLEAN),
                        new DataTypeResolver.DataTypeEntry(Types.BIGINT, MySqlParser.SERIAL)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.CollectionDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.ENUM)
                                .setSuffixTokens(MySqlParser.BINARY),
                        new DataTypeResolver.DataTypeEntry(Types.CHAR, MySqlParser.SET)
                                .setSuffixTokens(MySqlParser.BINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.SpatialDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(
                                Types.OTHER, MySqlParser.GEOMETRYCOLLECTION),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.GEOMCOLLECTION),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.LINESTRING),
                        new DataTypeResolver.DataTypeEntry(
                                Types.OTHER, MySqlParser.MULTILINESTRING),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOINT),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOLYGON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.POINT),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.POLYGON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.JSON),
                        new DataTypeResolver.DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.LongVarbinaryDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.BLOB, MySqlParser.LONG)
                                .setSuffixTokens(MySqlParser.VARBINARY)));
        dataTypeResolverBuilder.registerDataTypes(
                MySqlParser.LongVarcharDataTypeContext.class.getCanonicalName(),
                Arrays.asList(
                        new DataTypeResolver.DataTypeEntry(Types.VARCHAR, MySqlParser.LONG)
                                .setSuffixTokens(MySqlParser.VARCHAR)));

        return dataTypeResolverBuilder.build();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new CustomMySqlAntlrDdlParserListener(this, parsedEvents);
    }

    public List<SchemaChangeEvent> getAndClearParsedEvents() {
        List<SchemaChangeEvent> result = new ArrayList<>(parsedEvents);
        parsedEvents.clear();
        return result;
    }
}
