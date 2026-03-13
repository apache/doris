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
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class));
        SENSITIVE_KEY.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AWSGlueMetaStoreBaseProperties.class));
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
