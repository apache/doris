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

package org.apache.doris.datasource.credentials;

import java.util.Map;

/**
 * Interface for future FileIO-based credential extraction.
 * This interface is designed to be compatible with Iceberg/Paimon FileIO
 */
public interface CredentialExtractor {
    // AWS credential property keys for backend
    String BACKEND_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    String BACKEND_AWS_SECRET_KEY = "AWS_SECRET_KEY";
    String BACKEND_AWS_TOKEN = "AWS_TOKEN";
    String BACKEND_AWS_ENDPOINT = "AWS_ENDPOINT";
    String BACKEND_AWS_REGION = "AWS_REGION";

    /**
     * Extract credentials from a generic properties map.
     *
     * @param properties properties map from any source (FileIO, Table IO, etc.)
     * @return extracted credentials as backend properties
     */
    Map<String, String> extractCredentials(Map<String, String> properties);
}
