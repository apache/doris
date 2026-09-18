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

package org.apache.doris.cdcclient.source.reader.mysql;

import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Date;
import java.util.Properties;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/** Preserves MySQL's special zero YEAR value in both snapshot and binlog records. */
public class MySqlYearConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final int BINLOG_ZERO_YEAR = 1900;

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!"YEAR".equalsIgnoreCase(field.typeName())) {
            return;
        }
        String qualifiedColumnName = field.dataCollection() + "." + field.name();
        registration.register(
                io.debezium.time.Year.builder(),
                value -> MySqlYearConverter.convertYear(value, qualifiedColumnName));
    }

    static Integer convertYear(Object value, String qualifiedColumnName) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.time.Year) {
            int year = ((java.time.Year) value).getValue();
            return year == BINLOG_ZERO_YEAR ? 0 : year;
        }
        if (value instanceof Date) {
            return ((Date) value).toLocalDate().getYear();
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.valueOf((String) value);
        }
        throw new IllegalArgumentException(
                "Unsupported value type for MySQL YEAR column "
                        + qualifiedColumnName
                        + ": "
                        + value.getClass().getName());
    }
}
