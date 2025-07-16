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
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

public class HdfsPropertiesUtils {
    private static final String URI_KEY = "uri";

    public static String validateAndGetUri(Map<String, String> props, Set<String> supportSchemas) throws UserException {
        if (props.isEmpty()) {
            throw new UserException("props is empty");
        }
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            throw new StoragePropertiesException("props must contain uri");
        }
        return validateAndNormalizeUri(uriStr, supportSchemas);
    }

    public static boolean validateUriIsHdfsUri(Map<String, String> props,
                                               Set<String> supportSchemas) {
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            return false;
        }
        try {
            URI uri = URI.create(uriStr);
            String schema = uri.getScheme();
            if (StringUtils.isBlank(schema)) {
                throw new IllegalArgumentException("Invalid uri: " + uriStr + ", extract schema is null");
            }
            return isSupportedSchema(schema, supportSchemas);
        } catch (AnalysisException e) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr, e);
        }
    }

    public static String extractDefaultFsFromPath(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        try {
            URI uri = URI.create(filePath);
            return uri.getScheme() + "://" + uri.getAuthority();
        } catch (AnalysisException e) {
            throw new IllegalArgumentException("Invalid file path: " + filePath, e);
        }
    }

    public static String extractDefaultFsFromUri(Map<String, String> props, Set<String> supportSchemas) {
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            return null;
        }
        try {
            URI uri = URI.create(uriStr);
            if (!isSupportedSchema(uri.getScheme(), supportSchemas)) {
                return null;
            }
            return uri.getScheme() + "://" + uri.getAuthority();
        } catch (AnalysisException e) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr, e);
        }
    }

    public static String convertUrlToFilePath(String uriStr, Set<String> supportSchemas) throws UserException {
        return validateAndNormalizeUri(uriStr, supportSchemas);
    }

    /*
     * Extracts the URI value from the given properties.
     * If multiple URIs are specified (separated by commas), this method returns null.
     * Note: Some storage systems may support multiple URIs (e.g., for load balancing or multi-host),
     * but in the HDFS scenario, fs.defaultFS only supports a single URI.
     * Therefore, such a format is considered invalid for HDFS. so, just return null.
     */
    private static String getUri(Map<String, String> props) {
        String uriValue = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (uriValue == null) {
            return null;
        }
        String[] uris = uriValue.split(",");
        if (uris.length > 1) {
            return null;
        }
        return uriValue;
    }

    private static boolean isSupportedSchema(String schema, Set<String> supportSchema) {
        return schema != null && supportSchema.contains(schema.toLowerCase());
    }

    private static String validateAndNormalizeUri(String uriStr, Set<String> supportSchema) throws AnalysisException {
        if (StringUtils.isBlank(uriStr)) {
            throw new IllegalArgumentException("Properties 'uri' is required");
        }
        URI uri = URI.create(uriStr);
        String schema = uri.getScheme();
        if (StringUtils.isBlank(schema)) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr + ", extract schema is null");
        }
        if (!isSupportedSchema(schema, supportSchema)) {
            throw new IllegalArgumentException("Invalid export path:"
                    + schema + " , please use valid 'hdfs://' or 'viewfs://' path.");
        }
        return uri.getScheme() + "://" + uri.getAuthority() + uri.getPath();
    }
}
