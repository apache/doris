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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class S3PropertyUtils {


    private static final String URI_KEY = "uri";

    public static String constructEndpointFromUrl(Map<String, String> props,
                                                  String stringUsePathStyle, String stringForceParsingByStandardUri) {
        String uri = props.get(URI_KEY);
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        try {
            S3URI s3uri = S3URI.create(uri, usePathStyle, forceParsingByStandardUri);
            return s3uri.getEndpoint().orElse(null);
        } catch (UserException e) {
            return null;
        }
    }

    public static String constructRegionFromUrl(Map<String, String> props, String stringUsePathStyle,
                                                String stringForceParsingByStandardUri) {
        String uri = props.get(URI_KEY);
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);

        S3URI s3uri = null;
        try {
            s3uri = S3URI.create(uri, usePathStyle, forceParsingByStandardUri);
            return s3uri.getRegion().orElse(null);
        } catch (UserException e) {
            return null;
        }
    }

    public static String convertToS3Address(String path, String stringUsePathStyle,
                                            String stringForceParsingByStandardUri) throws UserException {
        if (StringUtils.isBlank(path)) {
            throw new UserException("path is null");
        }
        if (path.startsWith("s3://")) {
            return path;
        }

        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        S3URI s3uri = S3URI.create(path, usePathStyle, forceParsingByStandardUri);
        return "s3" + S3URI.SCHEME_DELIM + s3uri.getBucket() + S3URI.PATH_DELIM + s3uri.getKey();
    }

    public static String checkLoadPropsAndReturnUri(Map<String, String> props) throws UserException {
        if (props.isEmpty()) {
            throw new UserException("props is empty");
        }
        if (!props.containsKey(URI_KEY)) {
            throw new UserException("props must contain uri");
        }
        return props.get(URI_KEY);
    }
}
