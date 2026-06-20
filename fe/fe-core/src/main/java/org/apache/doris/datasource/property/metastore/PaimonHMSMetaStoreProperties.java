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

import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PaimonHMSMetaStoreProperties extends AbstractPaimonProperties {

    private HMSBaseProperties hmsBaseProperties;

    private static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY = "client-pool-cache.eviction-interval-ms";

    private static final String LOCATION_IN_PROPERTIES_KEY = "location-in-properties";

    @ConnectorProperty(
            names = {CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_KEY},
            required = false,
            description = "Setting the client's pool cache eviction interval(ms).")
    private long clientPoolCacheEvictionIntervalMs = TimeUnit.MINUTES.toMillis(5L);

    @ConnectorProperty(
            names = {LOCATION_IN_PROPERTIES_KEY},
            required = false,
            description = "Setting whether to use the location in the properties.")
    private boolean locationInProperties = false;


    @Override
    public String getPaimonCatalogType() {
        return "hms";
    }

    protected PaimonHMSMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        hmsBaseProperties = HMSBaseProperties.of(origProps);
        this.executionAuthenticator = new HadoopExecutionAuthenticator(hmsBaseProperties.getHmsAuthenticator());
    }
}
