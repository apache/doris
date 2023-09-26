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

package org.apache.doris.avro;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class S3Utils {
    private static final String SCHEMA_S3 = "s3";
    private static final String SCHEMA_HTTP = "http";
    private static final String SCHEMA_HTTPS = "https";
    private static final String SCHEME_DELIM = "://";
    private static final String PATH_DELIM = "/";
    private static final String QUERY_DELIM = "\\?";
    private static final String FRAGMENT_DELIM = "#";
    private static String bucket;
    private static String key;

    /**
     * eg:
     * s3:     s3://bucket1/path/to/file.txt
     * http:   http://10.10.10.1:9000/bucket1/to/file.txt
     * https:  https://10.10.10.1:9000/bucket1/to/file.txt
     * <p>
     * schema: s3,http,https
     * bucket: bucket1
     * key:    path/to/file.txt
     */
    public static void parseURI(String uri) throws IOException {
        if (StringUtils.isEmpty(uri)) {
            throw new IOException("s3 uri is empty.");
        }
        String[] schemeSplit = uri.split(SCHEME_DELIM);
        String rest;
        if (schemeSplit.length == 2) {
            if (schemeSplit[0].equalsIgnoreCase(SCHEMA_S3)) {
                // has scheme, eg: s3://bucket1/path/to/file.txt
                rest = schemeSplit[1];
                String[] authoritySplit = rest.split(PATH_DELIM, 2);
                if (authoritySplit.length < 1) {
                    throw new IOException("Invalid S3 URI. uri=" + uri);
                }
                bucket = authoritySplit[0];
                // support s3://bucket1
                key = authoritySplit.length == 1 ? "/" : authoritySplit[1];
            } else if (schemeSplit[0].equalsIgnoreCase(SCHEMA_HTTP) || schemeSplit[0].equalsIgnoreCase(SCHEMA_HTTPS)) {
                // has scheme, eg: http(s)://host/bucket1/path/to/file.txt
                rest = schemeSplit[1];
                String[] authoritySplit = rest.split(PATH_DELIM, 3);
                if (authoritySplit.length != 3) {
                    throw new IOException("Invalid S3 HTTP URI: uri=" + uri);
                }
                // authority_split[1] is host
                bucket = authoritySplit[1];
                key = authoritySplit[2];
            } else {
                throw new IOException("Invalid S3 HTTP URI: uri=" + uri);
            }

        } else if (schemeSplit.length == 1) {
            // no scheme, eg: path/to/file.txt
            bucket = ""; // unknown
            key = uri;
        } else {
            throw new IOException("Invalid S3 URI. uri=" + uri);
        }

        key = key.trim();
        if (StringUtils.isEmpty(key)) {
            throw new IOException("Invalid S3 URI. uri=" + uri);
        }
        // Strip query and fragment if they exist
        String[] querySplit = key.split(QUERY_DELIM);
        String[] fragmentSplit = querySplit[0].split(FRAGMENT_DELIM);
        key = fragmentSplit[0];
    }

    public static String getBucket() {
        return bucket;
    }

    public static String getKey() {
        return key;
    }

}
