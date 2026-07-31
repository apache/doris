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

package org.apache.doris.connector.adbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class AdbcConnectorPropertiesTest {

    private static Map<String, String> minimalProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AdbcConnectorProperties.DRIVER_URL, "libadbc_driver_flightsql.so");
        props.put(AdbcConnectorProperties.URI, "grpc://remote-fe:9090");
        return props;
    }

    @Test
    void driverOptionsKeepTheAdbcPrefix() {
        // The prefix is part of the option name, NOT a namespace to strip: ADBC's own option names already
        // begin with "adbc." (e.g. adbc.snowflake.sql.db), so stripping would send the driver a name it
        // does not know and the option would be silently ignored. BE applies the same rule; if the two
        // sides ever disagree, a catalog would plan against different driver settings than it reads with.
        Map<String, String> props = minimalProperties();
        props.put("adbc.adbc.snowflake.sql.db", "MYDB");
        props.put("adbc.custom.option", "v");

        Map<String, String> options = AdbcConnectorProperties.driverOptions(props);

        Assertions.assertEquals(Map.of("adbc.adbc.snowflake.sql.db", "MYDB", "adbc.custom.option", "v"),
                options);
    }

    @Test
    void nonPrefixedPropertiesAreNotPassedToTheDriver() {
        Map<String, String> props = minimalProperties();
        props.put("password", "secret");

        Assertions.assertEquals(Map.of(), AdbcConnectorProperties.driverOptions(props));
    }

    @Test
    void providerRejectsAMissingDriverUrlOrUri() {
        AdbcConnectorProvider provider = new AdbcConnectorProvider();
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(minimalProperties()));

        for (String required : new String[] {
                AdbcConnectorProperties.DRIVER_URL, AdbcConnectorProperties.URI}) {
            Map<String, String> props = minimalProperties();
            props.remove(required);
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(props));
            Assertions.assertTrue(e.getMessage().contains(required), e.getMessage());

            Map<String, String> blank = minimalProperties();
            blank.put(required, "   ");
            IllegalArgumentException blankError = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(blank));
            Assertions.assertTrue(blankError.getMessage().contains(required), blankError.getMessage());
        }
    }
}
