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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AzureAuthTypeTest {

    @ParameterizedTest
    @CsvSource({
            "SharedKey, SHARED_KEY",
            "sharedkey, SHARED_KEY",
            "SHARED_KEY, SHARED_KEY",
            "shared_key, SHARED_KEY",
            "'  SharedKey  ', SHARED_KEY",
            "SAS, SAS",
            "sas, SAS",
            "OAuth2, OAUTH2",
            "OAUTH2, OAUTH2",
            "oauth2, OAUTH2"
    })
    void parse_acceptsLegacyAndNativeValues(String input, AzureAuthType expected) {
        Assertions.assertEquals(expected, AzureAuthType.parse(input));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "unknown", "OAuth", "ManagedIdentity", "WorkloadIdentity"})
    void parse_rejectsUnsupportedValues(String input) {
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureAuthType.parse(input));

        Assertions.assertTrue(exception.getMessage().contains("Unsupported Azure auth_type"));
    }

    @Test
    void parse_doesNotEchoInvalidInput() {
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureAuthType.parse("credential-in-wrong-property"));

        Assertions.assertFalse(exception.getMessage().contains("credential-in-wrong-property"));
    }

    @Test
    void propertyValue_preservesLegacyValues() {
        Assertions.assertEquals(AzureFileSystemProperties.SHARED_KEY_AUTH, AzureAuthType.SHARED_KEY.propertyValue());
        Assertions.assertEquals(AzureFileSystemProperties.SAS_AUTH, AzureAuthType.SAS.propertyValue());
        Assertions.assertEquals(AzureFileSystemProperties.OAUTH2_AUTH, AzureAuthType.OAUTH2.propertyValue());
    }
}
