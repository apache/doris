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

package org.apache.doris.connector.hms;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for HMS client connection and pooling.
 *
 * <p>Constructed from the catalog properties map provided at CREATE CATALOG time.
 * All Hive-specific keys (metastore URI, auth type, etc.) are passed through to
 * HiveConf via {@link HmsConfHelper}.</p>
 */
public final class HmsClientConfig {

    /** Property key: HMS Thrift URI (e.g. "thrift://host:9083"). */
    public static final String HMS_URI_KEY = "hive.metastore.uris";

    /** Property key: metastore type — "hms" (default) or "dlf". */
    public static final String METASTORE_TYPE_KEY = "hive.metastore.type";

    /** Standard HMS (Thrift). */
    public static final String METASTORE_TYPE_HMS = "hms";

    /** Alibaba Cloud DLF. */
    public static final String METASTORE_TYPE_DLF = "dlf";

    /**
     * AWS Glue as an HMS-thrift metastore — REMOVED, no longer a routable type.
     * Retained only so the removal can be recognised and rejected explicitly: without a rejection this value
     * falls through to the plain-HMS default and silently connects somewhere the user never configured.
     */
    public static final String METASTORE_TYPE_GLUE_REMOVED = "glue";

    /**
     * Returns the rejection message when {@code properties} selects a metastore type that has been removed,
     * else null.
     *
     * <p>Callers throw their own exception type rather than this returning a throw: property validation must
     * raise {@link IllegalArgumentException} (the only type the catalog layer unwraps into a clean DdlException),
     * while the lazy client path raises DorisConnectorException like its neighbours.
     *
     * <p>Must be checked BEFORE the HMS URI requirement — a glue catalog carries no {@code hive.metastore.uris},
     * so the URI check would otherwise shadow this with an error that never mentions glue.
     */
    public static String removedMetastoreTypeError(Map<String, String> properties) {
        if (METASTORE_TYPE_GLUE_REMOVED.equalsIgnoreCase(properties.get(METASTORE_TYPE_KEY))) {
            return METASTORE_TYPE_KEY + " = " + METASTORE_TYPE_GLUE_REMOVED + " is no longer supported: "
                    + "AWS Glue as an HMS thrift metastore has been removed. Supported types: "
                    + METASTORE_TYPE_HMS + ", " + METASTORE_TYPE_DLF + ".";
        }
        return null;
    }

    private final Map<String, String> properties;
    private final int poolSize;

    /**
     * Creates a new HMS client configuration.
     *
     * @param properties all catalog properties (passed to HiveConf)
     * @param poolSize   max pool connections; 0 means no pooling
     */
    public HmsClientConfig(Map<String, String> properties, int poolSize) {
        this.properties = Objects.requireNonNull(properties, "properties");
        if (poolSize < 0) {
            throw new IllegalArgumentException("poolSize must be >= 0, got " + poolSize);
        }
        this.poolSize = poolSize;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public int getPoolSize() {
        return poolSize;
    }

    public String getMetastoreUri() {
        return properties.getOrDefault(HMS_URI_KEY, "");
    }

    public String getMetastoreType() {
        return properties.getOrDefault(METASTORE_TYPE_KEY, METASTORE_TYPE_HMS);
    }

    @Override
    public String toString() {
        return "HmsClientConfig{uri=" + getMetastoreUri()
                + ", type=" + getMetastoreType()
                + ", poolSize=" + poolSize + "}";
    }
}
