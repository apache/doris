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

package org.apache.doris.datasource.property.metastore;

import java.util.Map;

/**
 * PaimonAliyunDLFMetaStoreProperties
 *
 * <p>This class provides configuration support for using Apache Paimon with
 * Aliyun Data Lake Formation (DLF) as the metastore. Although DLF is not an
 * officially supported metastore type in Paimon, this implementation adapts
 * DLF by treating it as a Hive Metastore (HMS) underneath, enabling
 * interoperability with Paimon's HiveCatalog.
 *
 * <p>Key Characteristics:
 * <ul>
 *   <li>Internally uses HiveCatalog with custom HiveConf configured for Aliyun DLF.</li>
 *   <li>Relies on a DLF proxy metastore client to bridge DLF compatibility.</li>
 *   <li>Requires Aliyun OSS as the storage backend. Other storage types are not
 *       currently verified for compatibility.</li>
 * </ul>
 *
 * <p>Note: This is an internal extension and not an officially supported Paimon
 * metastore type. Future compatibility should be validated when upgrading Paimon
 * or changing storage backends.
 */
public class PaimonAliyunDLFMetaStoreProperties extends AbstractPaimonProperties {

    protected PaimonAliyunDLFMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        // Validate the DLF properties: AliyunDLFBaseProperties.of(...) runs checkAndInit() and throws on
        // missing dlf.access_key/dlf.secret_key/dlf.endpoint. The bound object is not retained because the
        // catalog is now built connector-side — only the validation side effect is needed here.
        AliyunDLFBaseProperties.of(origProps);
    }

    @Override
    public String getPaimonCatalogType() {
        return "dlf";
    }
}
