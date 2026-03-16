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

package org.apache.doris.foundation.util;

import java.util.Map;

/**
 * Basic map-to-string formatter with configurable separators and quoting.
 * No Doris-specific dependencies — only standard Java imports.
 *
 * <p>Subclasses can override {@link #shouldIncludeEntry} to filter entries
 * and {@link #formatValue} to customize value rendering (e.g. masking
 * sensitive values).  See {@code DatasourcePrintableMap} in fe-core.</p>
 */
public class BasicPrintableMap<K, V> {
    protected final Map<K, V> map;
    protected final String keyValueSeparator;
    protected final boolean withQuotation;
    protected final boolean wrap;
    protected final String entryDelimiter;

    public BasicPrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, String entryDelimiter) {
        this.map = map;
        this.keyValueSeparator = keyValueSeparator;
        this.withQuotation = withQuotation;
        this.wrap = wrap;
        this.entryDelimiter = entryDelimiter;
    }

    public BasicPrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap) {
        this(map, keyValueSeparator, withQuotation, wrap, ",");
    }

    @Override
    public String toString() {
        if (map == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (!shouldIncludeEntry(entry)) {
                continue;
            }
            if (!first) {
                appendDelimiter(sb);
            }
            appendEntry(sb, entry);
            first = false;
        }
        return sb.toString();
    }

    /**
     * Hook for subclasses to filter which entries to include in the output.
     * Default: include all entries.
     */
    protected boolean shouldIncludeEntry(Map.Entry<K, V> entry) {
        return true;
    }

    /**
     * Hook for subclasses to customize how a value is formatted.
     * Default: use String.valueOf(value).
     */
    protected String formatValue(Map.Entry<K, V> entry) {
        return String.valueOf(entry.getValue());
    }

    protected void appendEntry(StringBuilder sb, Map.Entry<K, V> entry) {
        if (withQuotation) {
            sb.append("\"");
        }
        sb.append(entry.getKey());
        if (withQuotation) {
            sb.append("\"");
        }
        sb.append(" ").append(keyValueSeparator).append(" ");
        if (withQuotation) {
            sb.append("\"");
        }
        sb.append(formatValue(entry));
        if (withQuotation) {
            sb.append("\"");
        }
    }

    protected void appendDelimiter(StringBuilder sb) {
        sb.append(entryDelimiter);
        if (wrap) {
            sb.append("\n");
        } else {
            sb.append(" ");
        }
    }
}
