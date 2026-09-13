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

import java.util.Map;
import java.util.Optional;

/** The complete SharedKey identity selected by the official Iceberg FileIO. */
final class AzureFileIOSharedKey {
    private static final String ACCOUNT_NAME = "adls.auth.shared-key.account.name";
    private static final String ACCOUNT_KEY = "adls.auth.shared-key.account.key";

    private final String accountName;
    private final String accountKey;

    private AzureFileIOSharedKey(String accountName, String accountKey) {
        this.accountName = accountName;
        this.accountKey = accountKey;
    }

    static Optional<AzureFileIOSharedKey> parse(Map<String, String> credentials) {
        String accountName = null;
        String accountKey = null;
        for (Map.Entry<String, String> entry : credentials.entrySet()) {
            boolean isName = ACCOUNT_NAME.equalsIgnoreCase(entry.getKey());
            boolean isKey = ACCOUNT_KEY.equalsIgnoreCase(entry.getKey());
            if (!isName && !isKey) {
                continue;
            }
            String value = entry.getValue();
            if (value == null || value.isBlank()) {
                throw new StoragePropertiesException("Azure FileIO SharedKey account name and key must not be empty");
            }
            String previous = isName ? accountName : accountKey;
            if (previous != null && !previous.equals(value)) {
                throw new StoragePropertiesException("Conflicting Azure FileIO SharedKey credential fields");
            }
            if (isName) {
                accountName = value;
            } else {
                accountKey = value;
            }
        }
        if (accountName == null && accountKey == null) {
            return Optional.empty();
        }
        if (accountName == null || accountKey == null) {
            throw new StoragePropertiesException("Azure FileIO SharedKey requires both account name and key");
        }
        return Optional.of(new AzureFileIOSharedKey(accountName, accountKey));
    }

    String accountName() {
        return accountName;
    }

    String accountKey() {
        return accountKey;
    }

    Map<String, String> properties() {
        return Map.of(ACCOUNT_NAME, accountName, ACCOUNT_KEY, accountKey);
    }
}
