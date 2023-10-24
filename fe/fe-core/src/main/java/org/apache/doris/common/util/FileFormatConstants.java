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

import java.util.regex.Pattern;

public class FileFormatConstants {
    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    public static final String DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR = "\001";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String FORMAT_CSV = "csv";
    public static final String FORMAT_CSV_WITH_NAMES = "csv_with_names";
    public static final String FORMAT_CSV_WITH_NAMES_AND_TYPES = "csv_with_names_and_types";
    public static final String FORMAT_HIVE_TEXT = "hive_text";
    public static final String FORMAT_PARQUET = "parquet";
    public static final String FORMAT_ORC = "orc";
    public static final String FORMAT_JSON = "json";
    public static final String FORMAT_AVRO = "avro";
    public static final String FORMAT_WAL = "wal";

    public static final String PROP_FORMAT = "format";
    public static final String PROP_COLUMN_SEPARATOR = "column_separator";
    public static final String PROP_LINE_DELIMITER = "line_delimiter";
    public static final String PROP_JSON_ROOT = "json_root";
    public static final String PROP_JSON_PATHS = "jsonpaths";
    public static final String PROP_STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String PROP_READ_JSON_BY_LINE = "read_json_by_line";
    public static final String PROP_NUM_AS_STRING = "num_as_string";
    public static final String PROP_FUZZY_PARSE = "fuzzy_parse";
    public static final String PROP_TRIM_DOUBLE_QUOTES = "trim_double_quotes";
    public static final String PROP_SKIP_LINES = "skip_lines";
    public static final String PROP_CSV_SCHEMA = "csv_schema";
    public static final String PROP_COMPRESS_TYPE = "compress_type";
    public static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    public static final String PROP_MAX_FILTER_RATIO = "max_filter_ratio";

    // decimal(p,s)
    public static final Pattern DECIMAL_TYPE_PATTERN = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
    // datetime(p)
    public static final Pattern DATETIME_TYPE_PATTERN = Pattern.compile("datetime\\((\\d+)\\)");

}
