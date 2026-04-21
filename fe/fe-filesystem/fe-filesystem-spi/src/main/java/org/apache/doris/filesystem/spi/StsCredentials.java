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

package org.apache.doris.filesystem.spi;

/**
 * Temporary credentials returned by a cloud STS (Security Token Service) operation.
 * Replaces the use of {@code Triple<String, String, String>} in legacy code.
 */
public final class StsCredentials {

    private final String accessKey;
    private final String secretKey;
    private final String securityToken;

    public StsCredentials(String accessKey, String secretKey, String securityToken) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.securityToken = securityToken;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getSecurityToken() {
        return securityToken;
    }
}
