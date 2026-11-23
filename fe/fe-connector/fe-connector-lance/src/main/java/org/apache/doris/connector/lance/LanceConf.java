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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * This plugin's own settings file, {@code lance.conf}: the keys, their defaults, and the reads.
 *
 * <p>These are one-per-FE settings, unlike the per-catalog properties in {@link LanceCatalogProperties}.
 * Before Lance became a connector plugin the key below was an fe.conf {@code @ConfField} of the same name.
 * It is not read from fe.conf any more: the engine no longer hands new connector settings through fe.conf,
 * so every read passes a {@code null} legacy environment key and the upgrade notes tell administrators to
 * move the value here.
 */
public final class LanceConf {

    /**
     * Java Arrow memory, in bytes, that one catalog may use while it reads dataset metadata. Must be
     * positive.
     */
    public static final String CONF_ARROW_MEMORY_LIMIT_BYTES = "lance_catalog_arrow_memory_limit_bytes";

    public static final long DEFAULT_ARROW_MEMORY_LIMIT_BYTES = 256L * 1024 * 1024;

    private LanceConf() {
    }

    /** The Arrow memory limit of one catalog's metadata reads; see {@link #CONF_ARROW_MEMORY_LIMIT_BYTES}. */
    public static long arrowMemoryLimitBytes(ConnectorContext context) {
        String value = ConnectorConf.get(context, CONF_ARROW_MEMORY_LIMIT_BYTES, null,
                String.valueOf(DEFAULT_ARROW_MEMORY_LIMIT_BYTES));
        long limit;
        try {
            limit = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(CONF_ARROW_MEMORY_LIMIT_BYTES
                    + " in lance.conf must be a positive number of bytes, but was '" + value + "'");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException(CONF_ARROW_MEMORY_LIMIT_BYTES + " must be positive");
        }
        return limit;
    }
}
