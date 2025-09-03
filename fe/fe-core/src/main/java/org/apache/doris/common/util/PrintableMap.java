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

import org.apache.doris.datasource.property.ConnectorPropertiesUtils;
import org.apache.doris.datasource.property.constants.MCProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.metastore.AWSGlueMetaStoreBaseProperties;
import org.apache.doris.datasource.property.metastore.AliyunDLFBaseProperties;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.MinioProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;

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
        SENSITIVE_KEY.addAll(Arrays.asList(
                S3Properties.SECRET_KEY,
                S3Properties.Env.SECRET_KEY,
                MCProperties.SECRET_KEY));

        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AWSGlueMetaStoreBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(GCSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OBSProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(MinioProperties.class));

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
        if (hidePassword && SENSITIVE_KEY.contains(entry.getKey())) {
            sb.append(PASSWORD_MASK);
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
