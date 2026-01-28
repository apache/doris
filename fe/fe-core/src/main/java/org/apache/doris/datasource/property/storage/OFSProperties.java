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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OFSProperties extends HdfsProperties {

    public OFSProperties(Map<String, String> origProps) {
        super(Type.OFS, origProps, true);
    }

    public static boolean guessIsMe(Map<String, String> props) {
        if (MapUtils.isEmpty(props)) {
            return false;
        }
        return props.containsKey("fs.ofs.impl") || props.containsKey("fs.AbstractFileSystem.ofs.impl");
    }

    @Override
    public String getStorageName() {
        return "OFS";
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return validateUri(url);
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        String uri = loadProps.get("uri");
        return validateUri(uri);
    }

    private String validateUri(String uri) throws UserException {
        if (StringUtils.isBlank(uri)) {
            throw new UserException("The uri is empty.");
        }
        URI uriObj = URI.create(uri);
        if (uriObj.getScheme() == null) {
            throw new UserException("The uri scheme is empty.");
        }
        if (!uriObj.getScheme().equalsIgnoreCase("ofs")) {
            throw new UserException("The uri scheme is not ofs.");
        }
        return uriObj.toString();
    }

    @Override
    protected void extractUserOtherConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        userOtherConfig = new HashMap<>();
        origProps.forEach((key, value) -> {
            // used for broker
            if (key.startsWith("kerberos_")) {
                userOtherConfig.put(key, value);
            }
        });
    }

    @Override
    protected Set<String> schemas() {
        return ImmutableSet.of("ofs");
    }
}
