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

import org.apache.doris.common.maxcompute.MCProperties;
import org.apache.doris.datasource.property.metastore.AWSGlueMetaStoreBaseProperties;
import org.apache.doris.datasource.property.metastore.AliyunDLFBaseProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.util.BasicPrintableMap;

import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DatasourcePrintableMap<K, V> extends BasicPrintableMap<K, V> {
    private boolean hidePassword;
    private Set<String> additionalHiddenKeys = Sets.newHashSet();

    public static final Set<String> SENSITIVE_KEY;
    public static final Set<String> HIDDEN_KEY;
    public static final String PASSWORD_MASK = "*XXX";

    static {
        SENSITIVE_KEY = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        SENSITIVE_KEY.add("password");
        SENSITIVE_KEY.add("kerberos_keytab_content");
        SENSITIVE_KEY.add("bos_secret_accesskey");
        SENSITIVE_KEY.add("jdbc.password");
        SENSITIVE_KEY.add("elasticsearch.password");
        SENSITIVE_KEY.addAll(Arrays.asList(
                MCProperties.SECRET_KEY));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AWSGlueMetaStoreBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(IcebergRestProperties.class));
        // Inlined union of the legacy typed storage classes' @ConnectorProperty(sensitive = true)
        // key aliases (S3/GCS/Azure/OSS/OSS-HDFS/COS/OBS/Minio Properties). The set is
        // case-insensitive, so alias spellings differing only in case are listed once. Locked
        // against the legacy classes by DatasourcePrintableMapSensitiveKeysParityTest until the
        // legacy package is deleted; SPI providers additionally feed registerSensitiveKeys at
        // FE startup.
        SENSITIVE_KEY.addAll(Arrays.asList(
                "s3.access_key", "AWS_ACCESS_KEY", "access_key", "glue.access_key",
                "aws.glue.access-key", "client.credentials-provider.glue.access_key",
                "iceberg.rest.access-key-id", "s3.access-key-id",
                "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "glue.secret_key",
                "aws.glue.secret-key", "client.credentials-provider.glue.secret_key",
                "iceberg.rest.secret-access-key", "s3.secret-access-key",
                "gs.access_key", "gs.secret_key",
                "azure.account_name", "azure.access_key", "azure.account_key", "azure.secret_key",
                "azure.oauth2_client_secret",
                "oss.access_key", "oss.secret_key", "oss.session_token",
                "dlf.access_key", "dlf.catalog.accessKeyId", "dlf.secret_key", "dlf.catalog.secret_key",
                "fs.oss.accessKeyId", "fs.oss.accessKeySecret", "fs.oss.securityToken",
                "s3.session_token", "s3.session-token", "session_token", "AWS_TOKEN",
                "oss.hdfs.access_key", "oss.hdfs.secret_key",
                "cos.access_key", "cos.secret_key",
                "obs.access_key", "obs.secret_key",
                "minio.access_key", "minio.secret_key", "minio.session_token"));
        HIDDEN_KEY = Sets.newHashSet();
        // Exact literals of the legacy S3Properties.Env.FS_KEYS list.
        HIDDEN_KEY.addAll(Arrays.asList("AWS_ENDPOINT", "AWS_REGION", "AWS_ACCESS_KEY", "AWS_SECRET_KEY",
                "AWS_TOKEN", "AWS_ROOT_PATH", "AWS_BUCKET", "AWS_MAX_CONNECTIONS",
                "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS"));
    }

    public DatasourcePrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, String entryDelimiter) {
        super(map, keyValueSeparator, withQuotation, wrap, entryDelimiter);
        this.hidePassword = false;
    }

    public DatasourcePrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap) {
        this(map, keyValueSeparator, withQuotation, wrap, ",");
    }

    public DatasourcePrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, boolean hidePassword) {
        this(map, keyValueSeparator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    public DatasourcePrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, boolean hidePassword, boolean sorted) {
        this(sorted ? new TreeMap<>(map).descendingMap() : map, keyValueSeparator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    public void setAdditionalHiddenKeys(Set<String> additionalHiddenKeys) {
        this.additionalHiddenKeys = additionalHiddenKeys;
    }

    /**
     * Registers additional sensitive property key aliases to be masked when printing property maps.
     *
     * <p>fe-core is decoupled from the filesystem provider implementations at the Maven level, so it
     * cannot statically reference their typed properties classes the way the static block does for the
     * legacy {@code *Properties}. Instead, {@code FileSystemPluginManager} aggregates each loaded
     * provider's {@code sensitivePropertyKeys()} and registers them here at FE startup, before any
     * SHOW CREATE / error-log printing occurs.
     */
    public static void registerSensitiveKeys(Collection<String> keys) {
        if (keys == null) {
            return;
        }
        synchronized (SENSITIVE_KEY) {
            SENSITIVE_KEY.addAll(keys);
        }
    }

    @Override
    protected boolean shouldIncludeEntry(Map.Entry<K, V> entry) {
        return !HIDDEN_KEY.contains(entry.getKey()) && !additionalHiddenKeys.contains(entry.getKey());
    }

    @Override
    protected String formatValue(Map.Entry<K, V> entry) {
        if (hidePassword && SENSITIVE_KEY.contains(entry.getKey())) {
            return PASSWORD_MASK;
        }
        return super.formatValue(entry);
    }
}
