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

package org.apache.doris.datasource.property.constants;

public class CsvProperties {
    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    public static final String DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR = "\001";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String PROP_COLUMN_SEPARATOR = "column_separator";
    public static final String PROP_LINE_DELIMITER = "line_delimiter";

    public static final String PROP_SKIP_LINES = "skip_lines";
    public static final String PROP_CSV_SCHEMA = "csv_schema";
    public static final String PROP_COMPRESS_TYPE = "compress_type";
    public static final String PROP_TRIM_DOUBLE_QUOTES = "trim_double_quotes";

    public static final String PROP_ENCLOSE = "enclose";
}
