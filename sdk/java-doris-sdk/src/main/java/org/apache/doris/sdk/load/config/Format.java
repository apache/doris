package org.apache.doris.sdk.load.config;

import java.util.Map;

/**
 * Stream load data format interface.
 * Implementations: JsonFormat, CsvFormat.
 */
public interface Format {
    /** Returns the format type string, e.g. "json", "csv". */
    String getFormatType();

    /** Returns format-specific HTTP headers for the stream load request. */
    Map<String, String> getHeaders();
}
