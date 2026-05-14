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

package org.apache.doris.connector.hive;

/**
 * Maps Hive InputFormat class names to file format strings understood by BE.
 */
public enum HiveFileFormat {

    PARQUET("parquet"),
    ORC("orc"),
    TEXT("text"),
    JSON("json"),
    UNKNOWN("unknown");

    private final String formatName;

    HiveFileFormat(String formatName) {
        this.formatName = formatName;
    }

    public String getFormatName() {
        return formatName;
    }

    /**
     * Detects the file format from the Hive inputFormat class name.
     */
    public static HiveFileFormat fromInputFormat(String inputFormat) {
        if (inputFormat == null) {
            return UNKNOWN;
        }
        String lower = inputFormat.toLowerCase();
        if (lower.contains("parquet")) {
            return PARQUET;
        }
        if (lower.contains("orc")) {
            return ORC;
        }
        if (lower.contains("text") || lower.contains("lazySimple")
                || lower.contains("lazysimple")) {
            return TEXT;
        }
        if (lower.contains("json")) {
            return JSON;
        }
        // Many Hive tables use TextInputFormat as the default
        if (lower.contains("textinputformat")) {
            return TEXT;
        }
        return UNKNOWN;
    }

    /**
     * Detects the file format from the SerDe library class name.
     */
    public static HiveFileFormat fromSerDeLib(String serDeLib) {
        if (serDeLib == null) {
            return UNKNOWN;
        }
        if (serDeLib.contains("ParquetHiveSerDe") || serDeLib.contains("parquet")) {
            return PARQUET;
        }
        if (serDeLib.contains("OrcSerde") || serDeLib.contains("orc")) {
            return ORC;
        }
        if (serDeLib.contains("LazySimpleSerDe") || serDeLib.contains("OpenCSVSerde")
                || serDeLib.contains("MultiDelimitSerDe")) {
            return TEXT;
        }
        if (serDeLib.contains("JsonSerDe") || serDeLib.contains("json")) {
            return JSON;
        }
        return UNKNOWN;
    }

    /**
     * Determines the file format using both inputFormat and serDeLib,
     * preferring inputFormat when available.
     */
    public static HiveFileFormat detect(String inputFormat, String serDeLib) {
        HiveFileFormat fromInput = fromInputFormat(inputFormat);
        if (fromInput != UNKNOWN) {
            return fromInput;
        }
        return fromSerDeLib(serDeLib);
    }

    /**
     * Returns true if this format supports file splitting at byte boundaries.
     * Parquet and ORC have internal block structures that support splitting.
     * Text files can be split at line boundaries.
     */
    public boolean isSplittable() {
        return this == PARQUET || this == ORC || this == TEXT;
    }
}
