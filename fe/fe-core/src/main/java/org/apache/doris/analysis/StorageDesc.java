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

package org.apache.doris.analysis;

import org.apache.doris.backup.S3Storage;
import org.apache.doris.common.Config;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public abstract class StorageDesc {
    private static final Logger LOG = LoggerFactory.getLogger(StorageBackend.class);
    // for dfs
    public static final String HADOOP_JOB_UGI = "hadoop.job.ugi";
    public static final String HADOOP_JOB_GROUP_NAME = "hadoop.job.group.name";
    public static final String USER_NAME_KEY = "username";
    public static final String PASSWORD_KEY = "password";
    public static final String FS_DEFAULT_NAME = "fs.default.name";
    public static final String FS_HDFS_IMPL = "fs.hdfs.impl";
    public static final String FS_AFS_IMPL = "fs.afs.impl";
    public static final String DFS_AGENT_PORT = "dfs.agent.port";
    public static final String DFS_CLIENT_AUTH_METHOD = "dfs.client.auth.method";
    // for bos
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESS_KEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESS_KEY = "bos_secret_accesskey";
    public static final String FS_BOS_IMPL = "fs.bos.impl";
    public static final String FS_BOS_ACCESS_KEY = "fs.bos.access.key";
    public static final String FS_BOS_SECRET_ACCESS_KEY = "fs.bos.secret.access.key";
    public static final String FS_BOS_ENDPOINT = "fs.bos.endpoint";
    public static final String FS_BOS_MULTIPART_UPLOADS_BLOCK_SIZE = "fs.bos.multipart.uploads.block.size";

    protected StorageBackend.StorageType storageType;
    protected Map<String, String> properties;
    protected String name;
    protected boolean convertedToS3 = false;

    protected void tryConvertToS3() {
        if (!Config.enable_access_file_without_broker || storageType != StorageBackend.StorageType.BROKER) {
            return;
        }
        CaseInsensitiveMap ciProperties = new CaseInsensitiveMap();
        ciProperties.putAll(properties);
        if (StringUtils.isNotEmpty(ciProperties.get(BOS_ENDPOINT).toString()) &&
            StringUtils.isNotEmpty(ciProperties.get(BOS_ACCESS_KEY).toString()) &&
            StringUtils.isNotEmpty(ciProperties.get(BOS_SECRET_ACCESS_KEY).toString())) {
            // bos endpoint like http[s]://gz.bcebos.com, we want to extract region gz,
            // and convert to s3 endpoint http[s]://s3.gz.bcebos.com
            String bosEndpiont = ciProperties.get(BOS_ENDPOINT).toString();
            try {
                URI uri = new URI(bosEndpiont);
                String host = uri.getHost();
                String[] hostSplit = host.split("\\.");
                if (hostSplit.length < 3) {
                    return;
                }
                String region = hostSplit[0];
                String s3Endpoint = new URIBuilder(uri).setHost("s3." + host).build().toString();
                properties.clear();
                properties.put(S3Storage.S3_ENDPOINT, s3Endpoint);
                properties.put(S3Storage.S3_REGION, region);
                properties.put(S3Storage.S3_AK, ciProperties.get(BOS_ACCESS_KEY).toString());
                properties.put(S3Storage.S3_SK, ciProperties.get(BOS_SECRET_ACCESS_KEY).toString());
                storageType = StorageBackend.StorageType.S3;
                convertedToS3 = true;
                LOG.info("skip BROKER and access S3 directly.");
            } catch (URISyntaxException e) {
                LOG.warn(BOS_ENDPOINT + ": " + bosEndpiont + " is invalid.");
            }
        }
    }

    protected String convertPathToS3(String path) {
        if (!convertedToS3) {
            return path;
        }
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
