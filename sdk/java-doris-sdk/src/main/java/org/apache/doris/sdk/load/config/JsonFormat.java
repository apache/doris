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
