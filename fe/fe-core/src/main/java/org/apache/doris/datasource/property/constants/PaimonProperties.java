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

import org.apache.doris.common.credentials.CloudCredential;

import com.google.common.collect.Maps;

import java.util.Map;

public class PaimonProperties {
    public static final String WAREHOUSE = "warehouse";
    public static final String S3_PATH_STYLE = "s3.path.style.access";
    public static final String FILE_FORMAT = "file.format";
    public static final String PAIMON_PREFIX = "paimon.";
    public static final String PAIMON_CATALOG_TYPE = "metastore";
    public static final String HIVE_METASTORE_URIS = "uri";
    public static final String PAIMON_S3_ENDPOINT = "s3.endpoint";
    public static final String PAIMON_S3_ACCESS_KEY = "s3.access-key";
    public static final String PAIMON_S3_SECRET_KEY = "s3.secret-key";
    public static final String PAIMON_OSS_ENDPOINT = org.apache.hadoop.fs.aliyun.oss.Constants.ENDPOINT_KEY;
    public static final String PAIMON_OSS_ACCESS_KEY = org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
    public static final String PAIMON_OSS_SECRET_KEY = org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
    public static final String PAIMON_HMS_CATALOG = "hive";
    public static final String PAIMON_FILESYSTEM_CATALOG = "filesystem";


    public static Map<String, String> convertToS3Properties(Map<String, String> properties,
            CloudCredential credential) {
        Map<String, String> s3Properties = Maps.newHashMap();
        s3Properties.put(PAIMON_S3_ACCESS_KEY, properties.get(S3Properties.ACCESS_KEY));
        s3Properties.put(PAIMON_S3_SECRET_KEY, properties.get(S3Properties.SECRET_KEY));
        s3Properties.putAll(properties);
        // remove extra meta properties
        s3Properties.remove(S3Properties.ACCESS_KEY);
        s3Properties.remove(S3Properties.SECRET_KEY);
        return s3Properties;
    }

}
