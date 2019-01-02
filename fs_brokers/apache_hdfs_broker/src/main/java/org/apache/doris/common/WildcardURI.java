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

package org.apache.doris.common;

import org.apache.doris.broker.hdfs.BrokerException;
import org.apache.doris.thrift.TBrokerOperationStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/*
 * Path may include wildcard, like 2018[8-9]*, but these characters are not valid in URI,
 * So we first encode the path, except '/' and ':'.
 * When we get path, we need to decode the path first.
 * eg:
 * hdfs://host/testdata/20180[8-9]*;
 * -->
 * hdfs://host/testdata/20180%5B8-9%5D*
 * 
 * getPath() will return: /testdata/20180[8-9]*
 */
public class WildcardURI {
    private static Logger logger = LogManager.getLogger(WildcardURI.class.getName());

    private URI uri;

    public WildcardURI(String path) {
        try {
            String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.toString()).replaceAll("%3A",
                    ":").replaceAll("%2F", "/");
            uri = new URI(encodedPath);
            uri.normalize();
        } catch (UnsupportedEncodingException | URISyntaxException e) {
            logger.warn("failed to encoded uri: " + path, e);
            e.printStackTrace();
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                    e, "invalid input path {} ", path);
        }
    }

    public URI getUri() {
        return uri;
    }

    public String getPath() {
        try {
            return URLDecoder.decode(uri.getPath(), StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.warn("failed to get path: " + uri.getPath(), e);
            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
                    e, "failed to get path {} ", uri.getPath());
        }
    }

    public String getAuthority() {
        return uri.getAuthority();
    }
}
