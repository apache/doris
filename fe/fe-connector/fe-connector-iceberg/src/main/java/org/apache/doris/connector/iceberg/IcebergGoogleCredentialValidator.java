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

package org.apache.doris.connector.iceberg;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/** Checks the JSON fields the Google SDK consumes without resolving credential sources. */
final class IcebergGoogleCredentialValidator {
    private IcebergGoogleCredentialValidator() {
    }

    static void validate(Map<String, String> properties) {
        String json = properties.get("gcp.auth.credentials-json");
        // The SDK treats empty JSON as unset; ALTER uses it to fall back to a credential path or ADC.
        if (json == null || json.isEmpty()) {
            return;
        }
        GenericJson credential;
        try {
            // Use the SDK parser so duplicate fields have the same final type at validation and consumption.
            credential = new JsonObjectParser(GsonFactory.getDefaultInstance())
                    .parseAndClose(new StringReader(json), GenericJson.class);
        } catch (IOException | RuntimeException failure) {
            // Parser messages and causes may contain private keys; expose only the fixed property name.
            throw new IllegalArgumentException("Invalid gcp.auth.credentials-json");
        }
        if (credential == null || !(credential.get("type") instanceof String)) {
            throw new IllegalArgumentException("gcp.auth.credentials-json type must contain a string");
        }
        // The SDK casts these fields to String; its JSON-null sentinel is also a non-String value.
        // Keep credential type, file and endpoint selection with the SDK, as existing catalogs require.
        for (String field : new String[] {"client_id", "client_email", "private_key", "private_key_id", "project_id",
                "token_uri", "quota_project_id", "universe_domain"}) {
            if (credential.containsKey(field) && !(credential.get(field) instanceof String)) {
                throw new IllegalArgumentException("gcp.auth.credentials-json fields must contain strings");
            }
        }
    }
}
