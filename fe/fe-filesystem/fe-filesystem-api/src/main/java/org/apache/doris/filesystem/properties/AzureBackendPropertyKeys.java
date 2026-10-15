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

package org.apache.doris.filesystem.properties;

/**
 * Neutral keys of native Azure access in {@link BackendStorageProperties#toMap()}.
 *
 * <p>The Azure provider writes them next to the legacy S3-style keys, and the fe-core adapter
 * turns them into the typed BE request ({@code TAzureClientConfig}). They are present only when
 * the provider supports native BE access for the configured authentication; a map without
 * {@link #AUTH_TYPE} carries no native Azure access.
 */
public final class AzureBackendPropertyKeys {

    /** One of {@link #AUTH_SHARED_KEY}, {@link #AUTH_SAS} or {@link #AUTH_OAUTH2}. */
    public static final String AUTH_TYPE = "AZURE_AUTH_TYPE";
    public static final String ENDPOINT = "AZURE_ENDPOINT";
    public static final String ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    /** Optional; when absent BE takes the container from each file location. */
    public static final String CONTAINER = "AZURE_CONTAINER";

    public static final String ACCOUNT_KEY = "AZURE_ACCOUNT_KEY";
    /** SAS query string without the leading '?'. */
    public static final String SAS_TOKEN = "AZURE_SAS_TOKEN";
    public static final String TENANT_ID = "AZURE_TENANT_ID";
    public static final String CLIENT_ID = "AZURE_CLIENT_ID";
    public static final String CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    /** Root of the Entra ID authority, e.g. {@code https://login.microsoftonline.com/}. */
    public static final String AUTHORITY_HOST = "AZURE_AUTHORITY_HOST";

    public static final String AUTH_SHARED_KEY = "SharedKey";
    public static final String AUTH_SAS = "SAS";
    public static final String AUTH_OAUTH2 = "OAuth2";

    private AzureBackendPropertyKeys() {
    }
}
