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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.URI;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HdfsPropertiesUtils {
    private static final String URI_KEY = "uri";

    private static Set<String> supportSchema = new HashSet<>();

    static {
        supportSchema.add("hdfs");
        supportSchema.add("viewfs");
    }

    public static String checkLoadPropsAndReturnUri(Map<String, String> props) throws UserException {
        if (props.isEmpty()) {
            throw new UserException("props is empty");
        }
        if (!props.containsKey(URI_KEY)) {
            throw new UserException("props must contain uri");
        }
        String uriStr = props.get(URI_KEY);
        return convertAndCheckUri(uriStr);
    }


    public static String convertUrlToFilePath(String uriStr) throws UserException {

        return convertAndCheckUri(uriStr);
    }

    public static String constructDefaultFsFromUri(Map<String, String> props) {
        if (props.isEmpty()) {
            return null;
        }
        if (!props.containsKey(URI_KEY)) {
            return null;
        }
        String uriStr = props.get(URI_KEY);
        if (StringUtils.isBlank(uriStr)) {
            return null;
        }
        URI uri = null;
        try {
            uri = URI.create(uriStr);
        } catch (AnalysisException e) {
            return null;
        }
        String schema = uri.getScheme();
        if (StringUtils.isBlank(schema)) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr + "extract schema is null");
        }
        if (!supportSchema.contains(schema.toLowerCase())) {
            throw new IllegalArgumentException("Invalid export path:"
                    + schema + " , please use valid 'hdfs://' or 'viewfs://' path.");
        }
        return uri.getScheme() + "://" + uri.getAuthority();
    }

    private static String convertAndCheckUri(String uriStr) throws AnalysisException {
        if (StringUtils.isBlank(uriStr)) {
            throw new IllegalArgumentException("uri is null, pls check your params");
        }
        URI uri = URI.create(uriStr);
        String schema = uri.getScheme();
        if (StringUtils.isBlank(schema)) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr + "extract schema is null");
        }
        if (!supportSchema.contains(schema.toLowerCase())) {
            throw new IllegalArgumentException("Invalid export path:"
                    + schema + " , please use valid 'hdfs://' or 'viewfs://' path.");
        }
        return uri.getScheme() + "://" + uri.getAuthority() + uri.getPath();
    }
}
