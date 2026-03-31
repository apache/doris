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
 * JSON format configuration for stream load.
 * Supports JSON Lines (one object per line) and JSON Array formats.
 */
public class JsonFormat implements Format {

    public enum Type {
        /** JSON Lines: one JSON object per line, e.g. {"a":1}\n{"a":2} */
        OBJECT_LINE,
        /** JSON Array: a single JSON array, e.g. [{"a":1},{"a":2}] */
        ARRAY
    }

    private final Type type;

    public JsonFormat(Type type) {
        this.type = type;
    }

    @Override
    public String getFormatType() {
        return "json";
    }

    @Override
    public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("format", "json");
        if (type == Type.OBJECT_LINE) {
            headers.put("strip_outer_array", "false");
            headers.put("read_json_by_line", "true");
        } else {
            headers.put("strip_outer_array", "true");
        }
        return headers;
    }

    public Type getType() {
        return type;
    }
}
