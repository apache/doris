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

package org.apache.doris.nereids.properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * select hint UNIQUE_KEYS.
 */
public class SelectHintUniqueKeys extends SelectHint {
    // table name -> unique key definitions
    private final Map<String, List<List<String>>> uniqueKeys;

    /**
     * Constructor for UNIQUE_KEYS select hint.
     */
    public SelectHintUniqueKeys(String hintName, Map<String, List<List<String>>> uniqueKeys) {
        super(hintName);
        Map<String, List<List<String>>> copied = new LinkedHashMap<>();
        for (Map.Entry<String, List<List<String>>> entry : uniqueKeys.entrySet()) {
            List<List<String>> keySets = entry.getValue().stream()
                    .map(ImmutableList::copyOf)
                    .collect(ImmutableList.toImmutableList());
            copied.put(entry.getKey(), keySets);
        }
        this.uniqueKeys = ImmutableMap.copyOf(copied);
    }

    public Map<String, List<List<String>>> getUniqueKeys() {
        return uniqueKeys;
    }

    @Override
    public String toString() {
        String kvString = uniqueKeys.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(keySet -> entry.getKey() + "='" + String.join(", ", keySet) + "'"))
                .collect(Collectors.joining(", "));
        return super.getHintName() + "(" + kvString + ")";
    }
}

