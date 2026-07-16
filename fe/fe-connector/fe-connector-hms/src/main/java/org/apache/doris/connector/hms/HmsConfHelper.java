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
        // Pin the conf classloader to the plugin loader, mirroring PaimonCatalogFactory.assembleHiveConf.
        // HiveMetaStoreClient.loadFilterHooks resolves metastore.filter.hook via Configuration.getClass, which
        // uses the conf's OWN classLoader field (= the thread-context CL captured at new HiveConf() above), NOT
        // the live TCCL. createHiveConf runs in the ThriftHmsClient constructor on the FE query thread, BEFORE
        // ThriftHmsClient.doAs pins the TCCL, so that captured CL is still the parent 'app' loader (fe-core's own
        // hive-metastore copy). HiveMetaStoreClient later copies this conf (new Configuration(hiveConf) copies the
        // classLoader field), so under child-first plugin loading it resolves DefaultMetaStoreFilterHookImpl from
        // the parent while MetaStoreFilterHook is child-loaded, giving "class DefaultMetaStoreFilterHookImpl not
        // MetaStoreFilterHook" and failing client creation before any metastore RPC. doAs pins the LIVE TCCL
        // (fixes SecurityUtil.<clinit>) but cannot fix this conf-cached CL. Pinning here keeps the whole
        // hive-metastore class graph in one loader.
        hiveConf.setClassLoader(HmsConfHelper.class.getClassLoader());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        // A kerberized HMS requires SASL transport on the metastore Thrift connection. The legacy fe-core
        // HMSBaseProperties.initHadoopAuthenticator auto-enabled hive.metastore.sasl.enabled whenever the
        // metastore/hadoop auth was kerberos; preserve that here so a catalog that only declares kerberos auth
        // (without an explicit hive.metastore.sasl.enabled) still negotiates SASL, instead of opening a plain
        // TSocket that a kerberized metastore drops with TTransportException.
        if ("kerberos".equalsIgnoreCase(properties.get("hadoop.security.authentication"))
                || "kerberos".equalsIgnoreCase(properties.get("hive.metastore.authentication.type"))) {
            hiveConf.set("hive.metastore.sasl.enabled", "true");
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
