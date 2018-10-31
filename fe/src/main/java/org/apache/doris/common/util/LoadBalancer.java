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

package org.apache.doris.common.util;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class LoadBalancer<K> {

    private Map<K, Long> balanceMap;
    private static final Long MAX_VLAUE = Long.MAX_VALUE;
    private static final Long DEFAULT_VALUE = 0L;
    private final Long step;

    public LoadBalancer(Long step) {
        balanceMap = Maps.newHashMap();
        this.step = step;
    }

    public K chooseKey(final List<K> keys) {
        Long minValue = MAX_VLAUE;
        K chosenKey = null;
        for (K key : keys) {
            Long v = findOrInsert(key);
            if (v < minValue) {
                minValue = v;
                chosenKey = key;
            }
        }

        // update
        balanceMap.put(chosenKey, minValue + step);
        return chosenKey;
    }

    private Long findOrInsert(final K key) {
        Long value = balanceMap.get(key);
        if (value == null) {
            balanceMap.put(key, DEFAULT_VALUE);
            value = DEFAULT_VALUE;
        }
        return value;
    }

    @Override
    public String toString() {
        return balanceMap.toString();
    }
}
