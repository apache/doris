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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hudi.common.util.MapUtils;

import java.util.Map;
import java.util.Set;

public class HttpProperties extends StorageProperties {
    private static final ImmutableSet<String> HTTP_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(StorageProperties.FS_HTTP_SUPPORT)
            .build();

    public HttpProperties(Map<String, String> origProps) {
        super(Type.HTTP, origProps);
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return origProps;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        if (url == null || (!url.startsWith("http://") && !url.startsWith("https://") && !url.startsWith("hf://"))) {
            throw new UserException("Invalid http/hf url: " + url);
        }
        return url;
    }

    @Override
    public String validateAndGetUri(Map<String, String> props) throws UserException {
        String url = props.get(URI_KEY);
        return validateAndNormalizeUri(url);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        return !MapUtils.isNullOrEmpty(props)
            && HTTP_PROPERTIES.stream().anyMatch(props::containsKey);
    }

    public String getUri() {
        return origProps.get(URI_KEY);
    }

    @Override
    public String getStorageName() {
        return "http";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        // not used
        hadoopStorageConfig = null;
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("http");
    }

    public Map<String, String> getHeaders() {
        Map<String, String> headers = Maps.newHashMap();
        for (Map.Entry<String, String> entry : origProps.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith("http.header.")) {
                String headerKey = entry.getKey().substring("http.header.".length());
                headers.put(headerKey, entry.getValue());
            }
        }
        return headers;
    }
}
