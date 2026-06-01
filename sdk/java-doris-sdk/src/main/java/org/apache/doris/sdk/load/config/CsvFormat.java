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

package org.apache.doris.sdk.load.config;

import java.util.HashMap;
import java.util.Map;

/**
 * CSV format configuration for stream load.
 */
public class CsvFormat implements Format {

    private final String columnSeparator;
    private final String lineDelimiter;

    public CsvFormat(String columnSeparator, String lineDelimiter) {
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
    }

    @Override
    public String getFormatType() {
        return "csv";
    }

    @Override
    public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("format", "csv");
        headers.put("column_separator", columnSeparator);
        headers.put("line_delimiter", lineDelimiter);
        return headers;
    }

    public String getColumnSeparator() { return columnSeparator; }
    public String getLineDelimiter() { return lineDelimiter; }
}
