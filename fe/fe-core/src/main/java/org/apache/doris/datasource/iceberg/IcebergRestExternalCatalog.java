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

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.credentials.DataLakeAWSCredentialsProvider;
import org.apache.doris.datasource.iceberg.rest.DorisIcebergRestResolvedIO;
import org.apache.doris.datasource.property.PropertyConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog {

    public IcebergRestExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        icebergCatalogType = ICEBERG_REST;
        Map<String, String> restProperties = new HashMap<>();
        String restUri = catalogProperty.getProperties().getOrDefault(CatalogProperties.URI, "");
        restProperties.put(CatalogProperties.URI, restUri);
        restProperties.put(CatalogProperties.FILE_IO_IMPL, DorisIcebergRestResolvedIO.class.getName());
        restProperties.putAll(catalogProperty.getProperties());

        Configuration conf = replaceS3Properties(getConfiguration());

        RESTCatalog restCatalog = new RESTCatalog();
        restCatalog.setConf(conf);
        restCatalog.initialize(icebergCatalogType, restProperties);
        catalog = restCatalog;
    }

    private Configuration replaceS3Properties(Configuration conf) {
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        String credentials = catalogProperties
                .getOrDefault(Constants.AWS_CREDENTIALS_PROVIDER, DataLakeAWSCredentialsProvider.class.getName());
        conf.set(Constants.AWS_CREDENTIALS_PROVIDER, credentials);
        String usePahStyle = catalogProperties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "true");
        // Set path style
        conf.set(PropertyConverter.USE_PATH_STYLE, usePahStyle);
        conf.set(Constants.PATH_STYLE_ACCESS, usePahStyle);
        // Get AWS client retry limit
        conf.set(Constants.RETRY_LIMIT, catalogProperties.getOrDefault(Constants.RETRY_LIMIT, "1"));
        conf.set(Constants.RETRY_THROTTLE_LIMIT, catalogProperties.getOrDefault(Constants.RETRY_THROTTLE_LIMIT, "1"));
        conf.set(Constants.S3GUARD_CONSISTENCY_RETRY_LIMIT,
                catalogProperties.getOrDefault(Constants.S3GUARD_CONSISTENCY_RETRY_LIMIT, "1"));
        return conf;
    }
}
