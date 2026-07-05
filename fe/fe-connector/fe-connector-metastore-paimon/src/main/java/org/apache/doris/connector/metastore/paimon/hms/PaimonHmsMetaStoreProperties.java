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

package org.apache.doris.connector.metastore.paimon.hms;

import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * Paimon's Hive Metastore (HMS) backend. The fields, {@code toHiveConfOverrides}, and the shared
 * connection rules live in {@link AbstractHmsMetaStoreProperties}; paimon's {@link #validate()} adds the
 * {@code requireWarehouse()} that every paimon flavor enforces (legacy {@code AbstractPaimonProperties}),
 * then the shared connection check. Fire order (warehouse → uri → simple/kerberos auth) is byte-identical
 * to the pre-split {@code HmsMetaStorePropertiesImpl}.
 */
public final class PaimonHmsMetaStoreProperties extends AbstractHmsMetaStoreProperties {

    private PaimonHmsMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static PaimonHmsMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        PaimonHmsMetaStoreProperties props = new PaimonHmsMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        requireWarehouse();
        validateConnection();
    }
}
