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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;


public class PaimonFileExternalCatalog extends PaimonExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonFileExternalCatalog.class);

    public PaimonFileExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        super(catalogId, name, resource, props, comment);
    }

    @Override
    protected void initLocalObjectsImpl() {
        super.initLocalObjectsImpl();
        catalogType = PAIMON_FILESYSTEM;
        catalog = createCatalog();
    }

    @Override
    protected void setPaimonCatalogOptions(Map<String, String> properties, Map<String, String> options) {
        if (properties.containsKey(PaimonProperties.PAIMON_S3_ENDPOINT)) {
            options.put(PaimonProperties.PAIMON_S3_ENDPOINT,
                    properties.get(PaimonProperties.PAIMON_S3_ENDPOINT));
            options.put(PaimonProperties.PAIMON_S3_ACCESS_KEY,
                    properties.get(PaimonProperties.PAIMON_S3_ACCESS_KEY));
            options.put(PaimonProperties.PAIMON_S3_SECRET_KEY,
                    properties.get(PaimonProperties.PAIMON_S3_SECRET_KEY));
        } else if (properties.containsKey(PaimonProperties.PAIMON_OSS_ENDPOINT)) {
            options.put(PaimonProperties.PAIMON_OSS_ENDPOINT,
                    properties.get(PaimonProperties.PAIMON_OSS_ENDPOINT));
            options.put(PaimonProperties.PAIMON_OSS_ACCESS_KEY,
                    properties.get(PaimonProperties.PAIMON_OSS_ACCESS_KEY));
            options.put(PaimonProperties.PAIMON_OSS_SECRET_KEY,
                    properties.get(PaimonProperties.PAIMON_OSS_SECRET_KEY));
        } else if (properties.containsKey(CosProperties.ENDPOINT)) {
            options.put(PaimonProperties.PAIMON_S3_ENDPOINT,
                    properties.get(CosProperties.ENDPOINT));
            options.put(PaimonProperties.PAIMON_S3_ACCESS_KEY,
                    properties.get(CosProperties.ACCESS_KEY));
            options.put(PaimonProperties.PAIMON_S3_SECRET_KEY,
                    properties.get(CosProperties.SECRET_KEY));
            options.put(PaimonProperties.WAREHOUSE,
                    options.get(PaimonProperties.WAREHOUSE).replace("cosn://", "s3://"));
        } else if (properties.containsKey(ObsProperties.ENDPOINT)) {
            options.put(PaimonProperties.PAIMON_S3_ENDPOINT,
                    properties.get(ObsProperties.ENDPOINT));
            options.put(PaimonProperties.PAIMON_S3_ACCESS_KEY,
                    properties.get(ObsProperties.ACCESS_KEY));
            options.put(PaimonProperties.PAIMON_S3_SECRET_KEY,
                    properties.get(ObsProperties.SECRET_KEY));
            options.put(PaimonProperties.WAREHOUSE,
                    options.get(PaimonProperties.WAREHOUSE).replace("obs://", "s3://"));
        }

        if (properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            options.put(PaimonProperties.S3_PATH_STYLE, properties.get(PropertyConverter.USE_PATH_STYLE));
        }
    }
}
