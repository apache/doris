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

package org.apache.doris.connector.hms;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * HMS database metadata DTO using connector-SPI types.
 */
public final class HmsDatabaseInfo {

    private final String name;
    private final String locationUri;
    private final String comment;
    private final Map<String, String> parameters;

    public HmsDatabaseInfo(String name, String locationUri,
            String comment, Map<String, String> parameters) {
        this.name = Objects.requireNonNull(name, "name");
        this.locationUri = locationUri;
        this.comment = comment;
        this.parameters = parameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(parameters);
    }

    public String getName() {
        return name;
    }

    public String getLocationUri() {
        return locationUri;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "HmsDatabaseInfo{name='" + name + "'}";
    }
}
