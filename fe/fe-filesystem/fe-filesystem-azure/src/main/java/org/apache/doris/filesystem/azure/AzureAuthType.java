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

package org.apache.doris.filesystem.azure;

import org.apache.doris.foundation.property.StoragePropertiesException;

import org.apache.commons.lang3.StringUtils;

/** Azure authentication modes. Enum names are the canonical native backend values. */
public enum AzureAuthType {
    SHARED_KEY("SharedKey"),
    SAS("SAS"),
    OAUTH2("OAuth2");

    private final String propertyValue;

    AzureAuthType(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public String propertyValue() {
        return propertyValue;
    }

    public static AzureAuthType parse(String value) {
        String normalized = StringUtils.trim(value);
        for (AzureAuthType authType : values()) {
            if (authType.name().equalsIgnoreCase(normalized)
                    || authType.propertyValue.equalsIgnoreCase(normalized)) {
                return authType;
            }
        }
        throw new StoragePropertiesException(
                "Unsupported Azure auth_type. Expected SharedKey/SHARED_KEY, SAS or OAuth2/OAUTH2.");
    }
}
