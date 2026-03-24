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

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class FormatTest {

    @Test
    public void testJsonObjectLineHeaders() {
        JsonFormat fmt = new JsonFormat(JsonFormat.Type.OBJECT_LINE);
        Map<String, String> headers = fmt.getHeaders();
        assertEquals("json", headers.get("format"));
        assertEquals("true", headers.get("read_json_by_line"));
        assertEquals("false", headers.get("strip_outer_array"));
    }

    @Test
    public void testJsonArrayHeaders() {
        JsonFormat fmt = new JsonFormat(JsonFormat.Type.ARRAY);
        Map<String, String> headers = fmt.getHeaders();
        assertEquals("json", headers.get("format"));
        assertEquals("true", headers.get("strip_outer_array"));
        assertNull(headers.get("read_json_by_line"));
    }

    @Test
    public void testCsvHeaders() {
        CsvFormat fmt = new CsvFormat(",", "\\n");
        Map<String, String> headers = fmt.getHeaders();
        assertEquals("csv", headers.get("format"));
        assertEquals(",", headers.get("column_separator"));
        assertEquals("\\n", headers.get("line_delimiter"));
    }

    @Test
    public void testJsonFormatType() {
        JsonFormat fmt = new JsonFormat(JsonFormat.Type.OBJECT_LINE);
        assertEquals("json", fmt.getFormatType());
    }

    @Test
    public void testCsvFormatType() {
        CsvFormat fmt = new CsvFormat(",", "\\n");
        assertEquals("csv", fmt.getFormatType());
    }
}
