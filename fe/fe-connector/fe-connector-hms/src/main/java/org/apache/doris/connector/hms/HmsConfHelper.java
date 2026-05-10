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

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

/**
 * Utility for creating {@link HiveConf} from catalog properties.
 *
 * <p>This replaces the HiveConf initialization logic that was previously
 * embedded in fe-core's HMSBaseProperties. Connector plugins use this
 * to bootstrap a HiveConf instance from the flat property map provided
 * at CREATE CATALOG time.</p>
 */
public final class HmsConfHelper {

    private HmsConfHelper() {
    }

    /**
     * Create a {@link HiveConf} from catalog properties.
     *
     * <p>All key-value pairs from {@code properties} are set on the
     * HiveConf. This allows callers to pass through any Hive or Hadoop
     * configuration (metastore URI, auth settings, timeouts, etc.).</p>
     *
     * @param properties catalog properties map
     * @return a new HiveConf instance
     */
    public static HiveConf createHiveConf(Map<String, String> properties) {
        HiveConf hiveConf = new HiveConf();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        return hiveConf;
    }

    /**
     * Create a {@link HiveConf} with explicit metastore URI.
     *
     * @param metastoreUri the HMS Thrift URI (e.g. "thrift://host:9083")
     * @param properties   additional properties
     * @return a new HiveConf instance
     */
    public static HiveConf createHiveConf(String metastoreUri,
            Map<String, String> properties) {
        HiveConf hiveConf = createHiveConf(properties);
        if (metastoreUri != null && !metastoreUri.isEmpty()) {
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
        }
        return hiveConf;
    }
}
