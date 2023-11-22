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

package org.apache.doris.datasource.property;

import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.GCSProperties;
import org.apache.doris.datasource.property.constants.MinioProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.constants.S3Properties.Env;

import java.util.HashMap;
import java.util.Map;

public class S3ClientBEProperties {
    /**
     *  convert FE properties to BE S3 client properties
     *  On BE, should use properties like AWS_XXX.
     */
    public static Map<String, String> getBeFSProperties(Map<String, String> properties) {
        if (properties.containsKey(MinioProperties.ENDPOINT)) {
            if (!properties.containsKey(MinioProperties.REGION)) {
                properties.put(MinioProperties.REGION, MinioProperties.DEFAULT_REGION);
            }
            return getBeAWSPropertiesFromS3(S3Properties.prefixToS3(properties));
        } else if (properties.containsKey(S3Properties.ENDPOINT)) {
            // s3,oss,cos,obs use this.
            return getBeAWSPropertiesFromS3(properties);
        } else if (properties.containsKey(ObsProperties.ENDPOINT)
                || properties.containsKey(OssProperties.ENDPOINT)
                || properties.containsKey(GCSProperties.ENDPOINT)
                || properties.containsKey(CosProperties.ENDPOINT)) {
            return getBeAWSPropertiesFromS3(S3Properties.prefixToS3(properties));
        }
        return properties;
    }

    private static Map<String, String> getBeAWSPropertiesFromS3(Map<String, String> properties) {
        Map<String, String> beProperties = new HashMap<>();
        String endpoint = properties.get(S3Properties.ENDPOINT);
        beProperties.put(S3Properties.Env.ENDPOINT, endpoint);
        String region = S3Properties.getRegionOfEndpoint(endpoint);
        beProperties.put(S3Properties.Env.REGION, properties.getOrDefault(S3Properties.REGION, region));
        if (properties.containsKey(S3Properties.ACCESS_KEY)) {
            beProperties.put(S3Properties.Env.ACCESS_KEY, properties.get(S3Properties.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.SECRET_KEY)) {
            beProperties.put(S3Properties.Env.SECRET_KEY, properties.get(S3Properties.SECRET_KEY));
        }
        if (properties.containsKey(S3Properties.SESSION_TOKEN)) {
            beProperties.put(S3Properties.Env.TOKEN, properties.get(S3Properties.SESSION_TOKEN));
        }
        if (properties.containsKey(S3Properties.ROOT_PATH)) {
            beProperties.put(S3Properties.Env.ROOT_PATH, properties.get(S3Properties.ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.BUCKET)) {
            beProperties.put(S3Properties.Env.BUCKET, properties.get(S3Properties.BUCKET));
        }
        if (properties.containsKey(S3Properties.MAX_CONNECTIONS)) {
            beProperties.put(Env.MAX_CONNECTIONS, properties.get(S3Properties.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.REQUEST_TIMEOUT_MS)) {
            beProperties.put(Env.REQUEST_TIMEOUT_MS, properties.get(S3Properties.REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.CONNECTION_TIMEOUT_MS)) {
            beProperties.put(Env.CONNECTION_TIMEOUT_MS, properties.get(S3Properties.CONNECTION_TIMEOUT_MS));
        }
        if (properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            beProperties.put(PropertyConverter.USE_PATH_STYLE, properties.get(PropertyConverter.USE_PATH_STYLE));
        }
        return beProperties;
    }
}
