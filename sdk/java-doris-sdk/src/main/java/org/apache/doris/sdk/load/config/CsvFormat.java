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
