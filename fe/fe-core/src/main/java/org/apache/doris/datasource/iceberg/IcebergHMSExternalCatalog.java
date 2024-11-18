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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.PropertyConverter;

import java.util.Map;

public class IcebergHMSExternalCatalog extends IcebergExternalCatalog {

    public IcebergHMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_HMS;
        catalog = IcebergUtils.createIcebergHiveCatalog(this, getName());
        if (preExecutionAuthenticator.getHadoopAuthenticator() == null) {
            AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(getConfiguration());
            HadoopAuthenticator authenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
            preExecutionAuthenticator.setHadoopAuthenticator(authenticator);
        }
    }
}

