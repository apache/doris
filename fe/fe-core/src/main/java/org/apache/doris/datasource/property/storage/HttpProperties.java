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

import com.uber.m3.util.ImmutableSet;
import org.apache.doris.common.UserException;
import org.apache.http.annotation.Immutable;
import org.apache.hudi.common.util.MapUtils;

import java.util.Map;

public class HttpProperties extends StorageProperties {
    public static final String PROP_URL = "uri";
    public static final String PROP_HTTP_SUPPORT = "fs.http.support";

    private static final ImmutableSet<String> LOCAL_PROPERTIES =
        new ImmutableSet.Builder<String>()
            .add(PROP_URL)
            .add(PROP_HTTP_SUPPORT)
            .build();

    public HttpProperties(Map<String, String> origProps) {
        super(Type.HTTP, origProps);
    }

    public static boolean canHandle(Map<String, String> props) {
        if (MapUtils.isNullOrEmpty(props)) {
            return false;
        }
        String uri = props.get(PROP_URL);
        return uri != null && (uri.startsWith("http://") || uri.startsWith("https://"))
            || props.containsKey(PROP_HTTP_SUPPORT);
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return origProps;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        if(url == null || (!url.startsWith("http://") && !url.startsWith("https://"))) {
            throw  new UserException("Invalid http url: " + url);
        }
        return url;
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        String url = loadProps.get(PROP_URL);
        return validateAndNormalizeUri(url);
    }


    @Override
    public String getStorageName() {
        return origProps.get(PROP_URL);
    }

    @Override
    public void initializeHadoopStorageConfig() {
        
    }

}
