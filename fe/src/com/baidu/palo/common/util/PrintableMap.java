// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.util;

import java.util.Iterator;
import java.util.Map;

public class PrintableMap<K, V> {
    private Map<K, V> map;
    private String keyValueSaperator;
    private boolean withQuotation;
    private boolean wrap;
    
    public PrintableMap(Map<K, V> map, String keyValueSaperator,
                        boolean withQuotation, boolean wrap) {
        this.map = map;
        this.keyValueSaperator = keyValueSaperator;
        this.withQuotation = withQuotation;
        this.wrap = wrap;
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
            sb.append(" ").append(keyValueSaperator).append(" ");
            if (withQuotation) {
                sb.append("\"");
            }
            sb.append(entry.getValue());
            if (withQuotation) {
                sb.append("\"");
            }
            if (iter.hasNext()) {
                if (wrap) {
                    sb.append(",\n");
                } else {
                    sb.append(", ");
                }
            }
        }
        return sb.toString();
    }
}
