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

package org.apache.doris.datasource.property.common;

public enum AwsCredentialsProviderMode {

    DEFAULT("DEFAULT"),

    ENV("ENV"),

    SYSTEM_PROPERTIES("SYSTEM_PROPERTIES"),

    WEB_IDENTITY("WEB_IDENTITY"),

    CONTAINER("CONTAINER"),

    INSTANCE_PROFILE("INSTANCE_PROFILE"),

    ANONYMOUS("ANONYMOUS");

    private final String mode;

    AwsCredentialsProviderMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }


    public static AwsCredentialsProviderMode fromString(String value) {
        if (value == null || value.isEmpty()) {
            return DEFAULT;
        }

        String normalized = value.trim().toUpperCase().replace('-', '_');

        switch (normalized) {
            case "ENV":
                return ENV;
            case "SYSTEM_PROPERTIES":
                return SYSTEM_PROPERTIES;
            case "WEB_IDENTITY":
                return WEB_IDENTITY;
            case "CONTAINER":
                return CONTAINER;
            case "INSTANCE_PROFILE":
                return INSTANCE_PROFILE;
            case "ANONYMOUS":
                return ANONYMOUS;
            case "DEFAULT":
                return DEFAULT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported AWS credentials provider mode: " + value);
        }
    }
}
