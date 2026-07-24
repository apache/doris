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
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.COSProperties;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.MinioProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSHdfsProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
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
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(S3Properties.class));
        // DLF 1.0 secret keys. Formerly reflected off AliyunDLFBaseProperties, removed with the DLF 1.0 thrift
        // metastore. Masking must outlive the feature: a DLF catalog created before the removal still replays from
        // the image (rejection deliberately fires at CREATE and at client creation, never during replay, so FE can
        // still start), so it remains listable and SHOW CREATE CATALOG still prints its stored properties. All four
        // former sensitive keys are enumerated here, byte-identical to the former reflection result (the class had
        // no superclass, so the walk contributed nothing else). The overlap with OSSProperties/OSSHdfsProperties
        // below is uneven and must NOT be relied on: they alias dlf.secret_key, but nothing else covers
        // dlf.catalog.accessKeySecret or either session-token alias, so omitting those would silently unmask them.
        SENSITIVE_KEY.add("dlf.secret_key");
        SENSITIVE_KEY.add("dlf.catalog.accessKeySecret");
        SENSITIVE_KEY.add("dlf.session_token");
        SENSITIVE_KEY.add("dlf.catalog.sessionToken");
        // Iceberg REST catalog secret keys. Formerly reflected off the fe-core IcebergRestProperties
        // (getSensitiveKeys). That class is being removed with the fe-core iceberg property cluster; its
        // authoritative copy now lives connector-side (fe-connector-metastore-iceberg
        // IcebergRestMetaStoreProperties), which fe-core cannot depend on. SHOW CREATE CATALOG masking must
        // still hide these, so all four former IcebergRestProperties sensitive keys are enumerated explicitly,
        // byte-identical to the former reflection result (its AbstractIcebergProperties/MetastoreProperties
        // superclass chain carries no sensitive keys). Note the overlap with S3Properties above is uneven and
        // must NOT be relied on: iceberg.rest.secret-access-key aliases S3Properties' (sensitive) secret-key,
        // but iceberg.rest.session-token aliases S3Properties' session-token field which is NOT sensitive, so
        // omitting it here would silently unmask it. Keep in sync with the connector's sensitive REST keys.
        SENSITIVE_KEY.add("iceberg.rest.oauth2.token");
        SENSITIVE_KEY.add("iceberg.rest.oauth2.credential");
        SENSITIVE_KEY.add("iceberg.rest.secret-access-key");
        SENSITIVE_KEY.add("iceberg.rest.session-token");
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(GCSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AzureProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSHdfsProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(COSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OBSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(MinioProperties.class));
        HIDDEN_KEY = Sets.newHashSet();
        HIDDEN_KEY.addAll(S3Properties.Env.FS_KEYS);
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
