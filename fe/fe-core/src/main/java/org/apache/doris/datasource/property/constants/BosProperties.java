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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.Config;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class BosProperties {
    private static final Logger LOG = LoggerFactory.getLogger(BosProperties.class);

    private static final String BOS_ENDPOINT = "bos_endpoint";
    private static final String BOS_ACCESS_KEY = "bos_accesskey";
    private static final String BOS_SECRET_ACCESS_KEY = "bos_secret_accesskey";

    public static boolean tryConvertBosToS3(Map<String, String> properties, StorageBackend.StorageType storageType) {
        if (!Config.enable_access_file_without_broker || storageType != StorageBackend.StorageType.BROKER) {
            return false;
        }
        CaseInsensitiveMap ciProperties = new CaseInsensitiveMap();
        ciProperties.putAll(properties);
        if (StringUtils.isNotEmpty(ciProperties.get(BOS_ENDPOINT).toString())
                && StringUtils.isNotEmpty(ciProperties.get(BOS_ACCESS_KEY).toString())
                && StringUtils.isNotEmpty(ciProperties.get(BOS_SECRET_ACCESS_KEY).toString())) {
            // bos endpoint like http[s]://gz.bcebos.com, we want to extract region gz,
            // and convert to s3 endpoint http[s]://s3.gz.bcebos.com
            String bosEndpiont = ciProperties.get(BOS_ENDPOINT).toString();
            try {
                URI uri = new URI(bosEndpiont);
                String host = uri.getHost();
                String[] hostSplit = host.split("\\.");
                if (hostSplit.length < 3) {
                    return false;
                }
                String region = hostSplit[0];
                String s3Endpoint = new URIBuilder(uri).setHost("s3." + host).build().toString();
                properties.clear();
                properties.put(S3Properties.Env.ENDPOINT, s3Endpoint);
                properties.put(S3Properties.Env.REGION, region);
                properties.put(S3Properties.Env.ACCESS_KEY, ciProperties.get(BOS_ACCESS_KEY).toString());
                properties.put(S3Properties.Env.SECRET_KEY, ciProperties.get(BOS_SECRET_ACCESS_KEY).toString());

                LOG.info("skip BROKER and access S3 directly.");
                return true;
            } catch (URISyntaxException e) {
                LOG.warn(BOS_ENDPOINT + ": " + bosEndpiont + " is invalid.");
            }
        }
        return false;
    }

    public static String convertPathToS3(String path) {
        try {
            URI orig = new URI(path);
            URI s3url = new URI("s3", orig.getRawAuthority(),
                    orig.getRawPath(), orig.getRawQuery(), orig.getRawFragment());
            return s3url.toString();
        } catch (URISyntaxException e) {
            return path;
        }
    }
}
