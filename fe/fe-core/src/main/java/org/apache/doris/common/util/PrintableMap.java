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

import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PrintableMap<K, V> {
    private Map<K, V> map;
    private String keyValueSeparator;
    private boolean withQuotation;
    private boolean wrap;
    private boolean hidePassword;
    private String entryDelimiter = ",";
    
    public static final Set<String> SENSITIVE_KEY;
    static {
        SENSITIVE_KEY = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        SENSITIVE_KEY.add("password");
        SENSITIVE_KEY.add("kerberos_keytab_content");
        SENSITIVE_KEY.add("bos_secret_accesskey");
    }
    
    public PrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, String entryDelimiter) {
        this.map = map;
        this.keyValueSeparator = keyValueSeparator;
        this.withQuotation = withQuotation;
        this.wrap = wrap;
        this.hidePassword = false;
        this.entryDelimiter = entryDelimiter;
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
                        boolean withQuotation, boolean wrap) {
        this(map, keyValueSeparator, withQuotation, wrap, ",");
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, boolean hidePassword) {
        this(map, keyValueSeparator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
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
            if (hidePassword && SENSITIVE_KEY.contains(entry.getKey())) {
                sb.append("*XXX");
            } else {
                sb.append(entry.getValue());
            }
            if (withQuotation) {
                sb.append("\"");
            }
            if (iter.hasNext()) {
                sb.append(entryDelimiter);
                if (wrap) {
                    sb.append("\n");
                } else {
                    sb.append(" ");
                }
            }
        }
        return sb.toString();
    }
}
