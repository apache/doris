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

import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.COSProperties;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.MinioProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSHdfsProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Parity guard for the sensitive/hidden key literals inlined into
 * {@link DatasourcePrintableMap}: the inlined set must keep masking every key alias the legacy
 * typed storage classes declared with {@code @ConnectorProperty(sensitive = true)}. Uses the
 * legacy classes as the oracle; delete this test together with the legacy package in Phase D.
 */
public class DatasourcePrintableMapSensitiveKeysParityTest {

    @Test
    public void testInlinedSensitiveKeysCoverLegacyStorageClasses() {
        Set<String> legacyUnion = new HashSet<>();
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(S3Properties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(GCSProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(AzureProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OSSHdfsProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(COSProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(OBSProperties.class));
        legacyUnion.addAll(ConnectorPropertiesUtils.getSensitiveKeys(MinioProperties.class));

        for (String key : legacyUnion) {
            Assertions.assertTrue(DatasourcePrintableMap.SENSITIVE_KEY.contains(key),
                    "Inlined SENSITIVE_KEY lost legacy sensitive key alias: " + key);
        }
    }

    @Test
    public void testInlinedHiddenKeysMatchLegacyEnvFsKeys() {
        // Oracle: the legacy S3Properties.Env.FS_KEYS list.
        Set<String> legacy = new HashSet<>(S3Properties.Env.FS_KEYS);
        Assertions.assertEquals(legacy, DatasourcePrintableMap.HIDDEN_KEY,
                "Inlined HIDDEN_KEY drifted from legacy S3Properties.Env.FS_KEYS");
    }
}
