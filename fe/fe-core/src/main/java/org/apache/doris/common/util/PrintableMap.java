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
import org.apache.doris.datasource.property.metastore.IcebergJdbcMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.LanceRestMetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.COSProperties;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.MinioProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSHdfsProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.OzoneProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PrintableMap<K, V> {
    private Map<K, V> map;
    private String keyValueSeparator;
    private boolean withQuotation;
    private boolean wrap;
    private boolean hidePassword;
    private String entryDelimiter = ",";
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
        SENSITIVE_KEY.add("ai.api_key");
        SENSITIVE_KEY.add("ai.embed.api_key");
        SENSITIVE_KEY.add("ai.embed.mm.api_key");
        SENSITIVE_KEY.add("lance.rest.bearer-token");
        SENSITIVE_KEY.add("lance.rest.api-key");
        SENSITIVE_KEY.addAll(Arrays.asList(
                MCProperties.ACCESS_KEY, MCProperties.SECRET_KEY, MCProperties.SESSION_TOKEN));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(S3Properties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AWSGlueMetaStoreBaseProperties.class));
        // OAuth bearer material must follow the same masking path as storage credentials in
        // audit SQL, SHOW CREATE output, and every other printable connector-property surface.
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(IcebergRestProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(GCSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AzureProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSHdfsProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(COSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OBSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(MinioProperties.class));
        // Collect every typed secret alias; the supplemental regex must never disable built-in protection.
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(IcebergJdbcMetaStoreProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(PaimonJdbcMetaStoreProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(PaimonRestMetaStoreProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(LanceRestMetastoreProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OzoneProperties.class));
        // Retained SDK aliases must remain protected even when this branch does not bind them.
        SENSITIVE_KEY.addAll(Arrays.asList("dlf.catalog.securityToken", "AZURE_ACCOUNT_KEY", "AZURE_CLIENT_SECRET",
                "COS_ACCESS_KEY", "COS_SECRET_KEY", "COS_SESSION_TOKEN", "COS_TOKEN",
                "OBS_ACCESS_KEY", "OBS_SECRET_KEY", "OBS_SESSION_TOKEN", "OBS_TOKEN",
                "OSS_ACCESS_KEY", "OSS_SECRET_KEY", "OSS_SESSION_TOKEN", "OSS_TOKEN"));
        HIDDEN_KEY = Sets.newHashSet();
        HIDDEN_KEY.addAll(S3Properties.Env.FS_KEYS);
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, String entryDelimiter) {
        this.map = map;
        this.keyValueSeparator = keyValueSeparator;
        this.withQuotation = withQuotation;
        this.wrap = wrap;
        this.hidePassword = false;
        this.entryDelimiter = entryDelimiter;
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
                        boolean withQuotation, boolean wrap) {
        this(map, keyValueSeparator, withQuotation, wrap, ",");
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
            boolean withQuotation, boolean wrap, boolean hidePassword) {
        this(map, keyValueSeparator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    public PrintableMap(Map<K, V> map, String keyValueSeparator,
                        boolean withQuotation, boolean wrap, boolean hidePassword, boolean sorted) {
        this(sorted ? new TreeMap<>(map).descendingMap() : map, keyValueSeparator, withQuotation, wrap);
        this.hidePassword = hidePassword;
    }

    public void setAdditionalHiddenKeys(Set<String> additionalHiddenKeys) {
        this.additionalHiddenKeys = additionalHiddenKeys;
    }

    /** Combines permanent secret names with the current supplemental name policy. */
    public static boolean isSensitiveKey(Object key) {
        if (!(key instanceof String)) {
            return false;
        }
        String name = (String) key;
        return SENSITIVE_KEY.contains(name) || DatasourcePropertyRedactor.isSensitiveName(name);
    }

    /** Masks a display copy while keeping runtime connection properties intact. */
    public static String redactProperty(Object key, Object value) {
        return DatasourcePropertyRedactor.redact(key, value);
    }

    /** Only for a task-selected singleton whose native path is already decoded. */
    public static String redactResolvedFileLocation(String location) {
        return DatasourcePropertyRedactor.redactResolvedFileLocation(location);
    }

    @Override
    public String toString() {
        if (map == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<K, V>> iter = showEntries().iterator();
        while (iter.hasNext()) {
            appendEntry(sb, iter.next());
            if (iter.hasNext()) {
                appendDelimiter(sb);
            }
        }
        return sb.toString();
    }

    private List<Map.Entry<K, V>> showEntries() {
        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        List<Map.Entry<K, V>> entries = new ArrayList<>();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            if (!HIDDEN_KEY.contains(entry.getKey()) && !additionalHiddenKeys.contains(entry.getKey())) {
                entries.add(entry);
            }
        }
        return entries;
    }

    private void appendEntry(StringBuilder sb, Map.Entry<K, V> entry) {
        if (withQuotation) {
            sb.append("\"");
        }
        sb.append(entry.getKey());
        if (withQuotation) {
            sb.append("\"");
        }
        sb.append(" ").append(keyValueSeparator).append(" ");
        if (withQuotation) {
            sb.append("\"");
        }
        if (hidePassword) {
            sb.append(redactProperty(entry.getKey(), entry.getValue()));
        } else {
            sb.append(entry.getValue());
        }
        if (withQuotation) {
            sb.append("\"");
        }
    }

    private void appendDelimiter(StringBuilder sb) {
        sb.append(entryDelimiter);
        if (wrap) {
            sb.append("\n");
        } else {
            sb.append(" ");
        }
    }
}
