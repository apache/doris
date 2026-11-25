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

package org.apache.doris.datasource.property.metastore;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

final class IcebergGoogleCredentialValidator {
    private IcebergGoogleCredentialValidator() {
    }

    static void validate(Map<String, String> properties) {
        // Only operator-controlled ADC may select credential files or external-account suppliers.
        if (properties.containsKey("gcp.auth.credentials-path")) {
            throw new IllegalArgumentException("gcp.auth.credentials-path is not allowed in catalog properties; "
                    + "configure application default credentials on the server instead");
        }
        String json = properties.get("gcp.auth.credentials-json");
        if (json == null) {
            return;
        }
        GenericJson credential;
        try {
            // Match the SDK parser so duplicate JSON fields cannot change type or endpoint after validation.
            credential = new JsonObjectParser(GsonFactory.getDefaultInstance())
                    .parseAndClose(new StringReader(json), GenericJson.class);
        } catch (IOException | RuntimeException e) {
            // Parser errors may include private keys; never attach the original message or cause.
            throw new IllegalArgumentException("Invalid gcp.auth.credentials-json");
        }
        if (credential == null || !"service_account".equals(credential.get("type"))) {
            throw new IllegalArgumentException("gcp.auth.credentials-json must contain service_account credentials");
        }
        // The SDK casts these fields to String; JSON null is also a non-String parser sentinel.
        // Reject bad types here so downstream failures cannot expose a credential-bearing property map.
        for (String field : new String[] {"client_id", "client_email", "private_key", "private_key_id", "project_id",
                "token_uri", "quota_project_id", "universe_domain"}) {
            if (credential.containsKey(field) && !(credential.get(field) instanceof String)) {
                throw new IllegalArgumentException("gcp.auth.credentials-json fields must contain strings");
            }
        }
        Object endpoint = credential.get("token_uri");
        if (endpoint != null && !"https://oauth2.googleapis.com/token".equals(endpoint)
                && !"https://accounts.google.com/o/oauth2/token".equals(endpoint)) {
            throw new IllegalArgumentException("gcp.auth.credentials-json must use a standard Google token endpoint");
        }
    }
}
