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

package org.apache.doris.common.credentials;

public class CloudCredentialWithEndpoint extends CloudCredential {

    private String endpoint;
    private String region;

    public CloudCredentialWithEndpoint(String endpoint, String region, CloudCredential credential) {
        this.endpoint = endpoint;
        this.region = region;
        setAccessKey(credential.getAccessKey());
        setSecretKey(credential.getSecretKey());
        setSessionToken(credential.getSessionToken());
    }

    public CloudCredentialWithEndpoint(String endpoint, String region, String accessKey, String secretKey) {
        this(endpoint, region, accessKey, secretKey, null);
    }

    public CloudCredentialWithEndpoint(String endpoint, String region, String accessKey,
                                       String secretKey, String token) {
        this.endpoint = endpoint;
        this.region = region;
        setAccessKey(accessKey);
        setSecretKey(secretKey);
        setSessionToken(token);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
