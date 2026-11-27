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

import org.apache.doris.connector.metastore.spi.AbstractDlfMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Map;

/** DLF connection facts composed by the shared HMS gateway backend. */
final class HmsDlfMetaStoreProperties extends AbstractDlfMetaStoreProperties {
    private HmsDlfMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    static HmsDlfMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        HmsDlfMetaStoreProperties properties = new HmsDlfMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(properties, raw);
        return properties;
    }

    @Override
    public void validate() {
        validateConnection();
    }
}
