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

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.File;
import java.util.LinkedHashMap;
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

    private static final String CONF_METASTORE_CLIENT_TIMEOUT_SECOND = "metastore_client_timeout_second";
    private static final String ENV_METASTORE_CLIENT_TIMEOUT_SECOND =
            "hive_metastore_client_timeout_second";
    private static final String ENV_HADOOP_CONFIG_DIR = "hadoop_config_dir";
    private static final String HADOOP_CONFIG_DIR_PROPERTY = "doris.hadoop.config.dir";

    private HmsConfHelper() {
    }

    /** Resolves the shared HMS timeout from this plugin's config, then the legacy FE environment. */
    public static String metastoreClientTimeoutSecond(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_METASTORE_CLIENT_TIMEOUT_SECOND,
                ENV_METASTORE_CLIENT_TIMEOUT_SECOND, "10");
    }

    /** Publishes the FE-global Hadoop resource directory before a connector performs its first HMS lookup. */
    public static void initializeHadoopConfigDir(ConnectorContext context) {
        String configured = context.getEnvironment().get(ENV_HADOOP_CONFIG_DIR);
        if (!isBlank(configured)) {
            // HMS resource lookup can precede storage binding, so it cannot rely on that lazy path to publish this.
            System.setProperty(HADOOP_CONFIG_DIR_PROPERTY, configured);
        }
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
        return createHiveConfWithResources("", properties);
    }

    /**
     * Create a {@link HiveConf} with resource files as the base and catalog properties as overrides.
     */
    public static HiveConf createHiveConfWithResources(String confResources, Map<String, String> properties) {
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
        addConfResources(hiveConf, confResources);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // A blank username was ignored by the legacy copy-if-present path; preserving a resource value (or
            // the "hadoop" default) avoids createRemoteUser("") failing before the first HMS RPC.
            if ("hadoop.username".equals(entry.getKey()) && isBlank(entry.getValue())) {
                continue;
            }
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        // A kerberized HMS requires SASL transport on the metastore Thrift connection. The legacy fe-core
        // HMSBaseProperties.initHadoopAuthenticator auto-enabled hive.metastore.sasl.enabled whenever the
        // metastore/hadoop auth was kerberos; preserve that here so a catalog that only declares kerberos auth
        // (without an explicit hive.metastore.sasl.enabled) still negotiates SASL, instead of opening a plain
        // TSocket that a kerberized metastore drops with TTransportException.
        String hmsAuthType = properties.get("hive.metastore.authentication.type");
        boolean explicitSimple = "simple".equalsIgnoreCase(hmsAuthType);
        if (explicitSimple) {
            // The explicit HMS mode is authoritative even when a base hive-site.xml enables SASL.
            hiveConf.set("hive.metastore.sasl.enabled", "false");
        } else if ("kerberos".equalsIgnoreCase(hmsAuthType)
                || (!explicitSimple
                        && "kerberos".equalsIgnoreCase(properties.get("hadoop.security.authentication")))) {
            hiveConf.set("hive.metastore.sasl.enabled", "true");
        }
        return hiveConf;
    }

    /**
     * Creates the lightweight Hadoop configuration used only for UGI resolution.
     */
    public static Configuration createHadoopConfWithResources(String confResources,
            Map<String, String> properties) {
        Configuration conf = new Configuration();
        conf.setClassLoader(HmsConfHelper.class.getClassLoader());
        addConfResources(conf, confResources);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if ("hadoop.username".equals(entry.getKey()) && isBlank(entry.getValue())) {
                continue;
            }
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    /**
     * Preserves connector-agnostic passthrough keys while applying canonical HMS overrides last.
     */
    public static Map<String, String> mergeCatalogProperties(Map<String, String> raw,
            Map<String, String> overrides) {
        Map<String, String> merged = new LinkedHashMap<>(raw);
        // Canonical parsing deliberately omits a blank username; remove the raw value so it cannot reappear
        // merely because the HMS client also preserves unrelated custom configuration keys.
        if (isBlank(merged.get("hadoop.username"))) {
            merged.remove("hadoop.username");
        }
        merged.putAll(overrides);
        return merged;
    }

    private static void addConfResources(Configuration conf, String confResources) {
        if (isBlank(confResources)) {
            return;
        }
        String baseDir = resolveHadoopConfigDir();
        for (String resource : confResources.split(",")) {
            File file = new File(baseDir, resource.trim());
            if (!file.isFile()) {
                throw new IllegalArgumentException("Config resource file does not exist: " + file);
            }
            conf.addResource(new Path(file.toURI()));
        }
    }

    private static String resolveHadoopConfigDir() {
        String configured = System.getProperty(HADOOP_CONFIG_DIR_PROPERTY);
        if (!isBlank(configured)) {
            return configured;
        }
        String home = System.getenv("DORIS_HOME");
        if (isBlank(home)) {
            home = System.getProperty("doris.home", "");
        }
        return home + "/plugins/hadoop_conf/";
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
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
