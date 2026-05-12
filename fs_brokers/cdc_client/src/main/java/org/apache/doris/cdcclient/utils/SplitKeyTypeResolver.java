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

package org.apache.doris.cdcclient.utils;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntSupplier;

/**
 * Casts JSON-round-tripped split-key values back to the Java type the JDBC driver returns
 * for that column. flink-cdc's chunk splitter uses strict {@code Objects.equals} internally,
 * so passing {@code Integer(3)} where it expects {@code Long(3)} (BIGINT column) silently
 * produces empty chunks.
 */
public final class SplitKeyTypeResolver {

    private static final ConcurrentMap<String, Integer> SQL_TYPE_CACHE = new ConcurrentHashMap<>();

    private SplitKeyTypeResolver() {}

    /** Returns cached sqlType, or computes via {@code resolver} and caches. */
    public static int getOrCompute(String cacheKey, IntSupplier resolver) {
        Integer cached = SQL_TYPE_CACHE.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        int v = resolver.getAsInt();
        SQL_TYPE_CACHE.put(cacheKey, v);
        return v;
    }

    /** Cast a Number to the Java type JDBC returns for the given {@link Types} code. */
    public static Object cast(Object v, int sqlType) {
        if (!(v instanceof Number)) {
            return v;
        }
        Number n = (Number) v;
        switch (sqlType) {
            case Types.BIGINT:
                return n.longValue();
            case Types.INTEGER:
                return n.intValue();
            case Types.SMALLINT:
                return n.shortValue();
            case Types.TINYINT:
                return n.byteValue();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new BigDecimal(n.toString());
            case Types.REAL:
            case Types.FLOAT:
                return n.floatValue();
            case Types.DOUBLE:
                return n.doubleValue();
            default:
                return v;
        }
    }
}
