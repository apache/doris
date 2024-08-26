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
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

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
    protected void initCatalog() {
        icebergCatalogType = ICEBERG_REST;

        Configuration conf = replaceS3Properties(getConfiguration());

        catalog = CatalogUtil.buildIcebergCatalog(getName(),
                convertToRestCatalogProperties(),
                conf);
    }

    private Configuration replaceS3Properties(Configuration conf) {
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        initS3Param(conf);
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

    private Map<String, String> convertToRestCatalogProperties() {

        Map<String, String> props = catalogProperty.getProperties();
        Map<String, String> restProperties = new HashMap<>(props);
        restProperties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        restProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        String restUri = props.getOrDefault(CatalogProperties.URI, "");
        restProperties.put(CatalogProperties.URI, restUri);
        if (props.containsKey(S3Properties.ENDPOINT)) {
            restProperties.put(S3FileIOProperties.ENDPOINT, props.get(S3Properties.ENDPOINT));
        }
        if (props.containsKey(S3Properties.ACCESS_KEY)) {
            restProperties.put(S3FileIOProperties.ACCESS_KEY_ID, props.get(S3Properties.ACCESS_KEY));
        }
        if (props.containsKey(S3Properties.SECRET_KEY)) {
            restProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, props.get(S3Properties.SECRET_KEY));
        }
        if (props.containsKey(S3Properties.REGION)) {
            restProperties.put(AwsClientProperties.CLIENT_REGION, props.get(S3Properties.REGION));
        }
        if (props.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            restProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, props.get(PropertyConverter.USE_PATH_STYLE));
        }
        return restProperties;
    }
}
