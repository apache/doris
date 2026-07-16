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

package org.apache.doris.connector.metastore.hms;

import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/**
 * The shared, engine-neutral Hive Metastore (HMS) metastore backend. All of the conf
 * ({@code toHiveConfOverrides}), the neutral {@code hive.*}/{@code hadoop.security.*} keys, and the
 * connection rules live in the shared {@link AbstractHmsMetaStoreProperties}. {@link #validate()} runs
 * ONLY the connection check — an HMS is a metastore, not a storage, so it imposes no warehouse requirement.
 */
public final class DefaultHmsMetaStoreProperties extends AbstractHmsMetaStoreProperties {

    private DefaultHmsMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static DefaultHmsMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        DefaultHmsMetaStoreProperties props = new DefaultHmsMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        validateConnection();
    }
}
