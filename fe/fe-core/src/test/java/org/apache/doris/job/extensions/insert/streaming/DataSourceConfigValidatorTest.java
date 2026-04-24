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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DataSourceConfigValidatorTest {

    private static final int PG_MAX_IDENTIFIER_LENGTH = 63;

    @Test
    public void testSlotNameAndPublicationNameAllowed() {
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
        props.put(DataSourceConfigKeys.SLOT_NAME, "my_custom_slot");
        props.put(DataSourceConfigKeys.PUBLICATION_NAME, "my_custom_pub");
        // Should not throw
        DataSourceConfigValidator.validateSource(props);
    }

    @Test
    public void testSlotNameAndPublicationNameNotRequired() {
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
        // Should not throw without slot_name and publication_name
        DataSourceConfigValidator.validateSource(props);
    }

    @Test
    public void testDefaultSlotNameFormat() {
        String slotName = DataSourceConfigKeys.defaultSlotName("12345");
        Assert.assertEquals("doris_cdc_12345", slotName);
        Assert.assertTrue(slotName.length() <= PG_MAX_IDENTIFIER_LENGTH);
    }

    @Test
    public void testDefaultPublicationNameFormat() {
        String pubName = DataSourceConfigKeys.defaultPublicationName("12345");
        Assert.assertEquals("doris_pub_12345", pubName);
        Assert.assertTrue(pubName.length() <= PG_MAX_IDENTIFIER_LENGTH);
    }

    @Test
    public void testDefaultNamesWithLargeJobId() {
        String maxJobId = String.valueOf(Long.MAX_VALUE);
        String slotName = DataSourceConfigKeys.defaultSlotName(maxJobId);
        String pubName = DataSourceConfigKeys.defaultPublicationName(maxJobId);
        Assert.assertTrue("Slot name should not exceed PG limit, actual: " + slotName.length(),
                slotName.length() <= PG_MAX_IDENTIFIER_LENGTH);
        Assert.assertTrue("Publication name should not exceed PG limit, actual: " + pubName.length(),
                pubName.length() <= PG_MAX_IDENTIFIER_LENGTH);
    }

    @Test
    public void testSlotNameRejectsInvalidPgIdentifiers() {
        String[] invalids = {
                "MyPub",              // uppercase
                "pub-name",           // hyphen
                "pub name",           // whitespace
                "pub'name",           // single quote
                "pub\"name",          // double quote
                "pub;drop",           // semicolon (SQL injection attempt)
                "1pub",               // starts with digit
                "",                   // empty
                "pub.name"            // dot
        };
        for (String invalid : invalids) {
            Map<String, String> props = new HashMap<>();
            props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
            props.put(DataSourceConfigKeys.SLOT_NAME, invalid);
            try {
                DataSourceConfigValidator.validateSource(props);
                Assert.fail("Expected IllegalArgumentException for slot_name='" + invalid + "'");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        }
    }

    @Test
    public void testPublicationNameRejectsInvalidPgIdentifiers() {
        String[] invalids = {"MyPub", "pub-name", "pub'x", "pub\"x", "1pub", "pub name"};
        for (String invalid : invalids) {
            Map<String, String> props = new HashMap<>();
            props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
            props.put(DataSourceConfigKeys.PUBLICATION_NAME, invalid);
            try {
                DataSourceConfigValidator.validateSource(props);
                Assert.fail("Expected IllegalArgumentException for publication_name='" + invalid + "'");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        }
    }

    @Test
    public void testSlotNameRejectsOverlongIdentifier() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= PG_MAX_IDENTIFIER_LENGTH; i++) {
            sb.append('a');
        }
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
        props.put(DataSourceConfigKeys.SLOT_NAME, sb.toString());
        try {
            DataSourceConfigValidator.validateSource(props);
            Assert.fail("Expected IllegalArgumentException for slot_name exceeding "
                    + PG_MAX_IDENTIFIER_LENGTH + " chars");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testSlotNameAcceptsValidPgIdentifiers() {
        String[] valids = {"my_slot", "_slot", "slot1", "a", "slot_with_digits_123"};
        for (String valid : valids) {
            Map<String, String> props = new HashMap<>();
            props.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/db");
            props.put(DataSourceConfigKeys.SLOT_NAME, valid);
            DataSourceConfigValidator.validateSource(props);
        }
    }
}
