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

package org.apache.doris.cdcclient.source.reader;

import org.apache.doris.job.cdc.request.JobBaseConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

public abstract class AbstractCdcSourceReader implements SourceReader {

    private final Map<String, Class<?>> splitKeyClassCache = new ConcurrentHashMap<>();

    protected abstract Class<?> probeSplitKeyClass(
            TableId tableId, Column splitColumn, JobBaseConfig jobConfig);

    protected Class<?> resolveSplitKeyClass(
            TableId tableId, Column splitColumn, JobBaseConfig jobConfig) {
        String key = tableId.identifier() + "." + splitColumn.name();
        return splitKeyClassCache.computeIfAbsent(
                key, k -> probeSplitKeyClass(tableId, splitColumn, jobConfig));
    }

    public static Object[] convertBounds(Object[] raw, Class<?> target, ObjectMapper mapper) {
        if (raw == null) {
            return null;
        }
        Object[] out = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) {
            out[i] = convertBound(raw[i], target, mapper);
        }
        return out;
    }

    private static Object convertBound(Object v, Class<?> target, ObjectMapper mapper) {
        if (v == null) {
            return null;
        }
        if (target.isInstance(v)) {
            return v;
        }
        String s = v.toString();
        if (target == java.sql.Date.class) {
            return java.sql.Date.valueOf(s);
        }
        if (target == java.sql.Timestamp.class) {
            return java.sql.Timestamp.valueOf(s);
        }
        if (target == java.sql.Time.class) {
            return java.sql.Time.valueOf(s);
        }
        return mapper.convertValue(v, target);
    }
}
