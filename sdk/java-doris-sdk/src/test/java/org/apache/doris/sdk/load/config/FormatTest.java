package org.apache.doris.sdk.load.config;

import org.junit.Test;
import java.util.Map;
import static org.junit.Assert.*;

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
