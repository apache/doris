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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link ConnectorSessionImpl} and {@link ConnectorSessionBuilder}.
 */
public class ConnectorSessionImplTest {

    @Test
    public void testGetPropertyFromCatalogProperties() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("jdbc_url", "jdbc:mysql://host:3306/db");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .build();

        Assertions.assertEquals("jdbc:mysql://host:3306/db",
                session.getProperty("jdbc_url", String.class));
    }

    @Test
    public void testGetPropertyFromSessionProperties() {
        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("file_split_size", "134217728");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withSessionProperties(sessionProps)
                .build();

        Assertions.assertEquals("134217728",
                session.getProperty("file_split_size", String.class));
    }

    @Test
    public void testSessionPropertyOverridesCatalogProperty() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("timeout", "3000");

        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("timeout", "5000");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .withSessionProperties(sessionProps)
                .build();

        // Session properties should take precedence
        Assertions.assertEquals("5000",
                session.getProperty("timeout", String.class));
        Assertions.assertEquals(5000,
                session.getProperty("timeout", Integer.class));
    }

    @Test
    public void testGetPropertyReturnsNullForMissingKey() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertNull(session.getProperty("nonexistent", String.class));
    }

    @Test
    public void testGetPropertyTypeConversions() {
        Map<String, String> props = new HashMap<>();
        props.put("int_val", "42");
        props.put("long_val", "9999999999");
        props.put("bool_val", "true");
        props.put("str_val", "hello");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(props)
                .build();

        Assertions.assertEquals(42, session.getProperty("int_val", Integer.class));
        Assertions.assertEquals(42, session.getProperty("int_val", int.class));
        Assertions.assertEquals(9999999999L, session.getProperty("long_val", Long.class));
        Assertions.assertEquals(9999999999L, session.getProperty("long_val", long.class));
        Assertions.assertEquals(true, session.getProperty("bool_val", Boolean.class));
        Assertions.assertEquals(true, session.getProperty("bool_val", boolean.class));
        Assertions.assertEquals("hello", session.getProperty("str_val", String.class));
    }

    @Test
    public void testGetPropertyRejectsUnsupportedType() {
        Map<String, String> props = new HashMap<>();
        props.put("val", "1.5");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(props)
                .build();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> session.getProperty("val", Double.class));
    }

    @Test
    public void testGetPropertyNullNameThrows() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertThrows(NullPointerException.class,
                () -> session.getProperty(null, String.class));
    }

    @Test
    public void testGetPropertyNullTypeThrows() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertThrows(NullPointerException.class,
                () -> session.getProperty("key", null));
    }

    @Test
    public void testCatalogAndSessionPropertiesAreImmutable() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("k1", "v1");
        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("k2", "v2");

        ConnectorSession session = ConnectorSessionBuilder.create()
                .withCatalogProperties(catalogProps)
                .withSessionProperties(sessionProps)
                .build();

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> session.getCatalogProperties().put("k3", "v3"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> session.getSessionProperties().put("k4", "v4"));
    }

    @Test
    public void testSessionMetadata() {
        ConnectorSession session = ConnectorSessionBuilder.create()
                .withQueryId("query-123")
                .withUser("admin")
                .withTimeZone("Asia/Shanghai")
                .withCatalogId(42)
                .withCatalogName("my_catalog")
                .build();

        Assertions.assertEquals("query-123", session.getQueryId());
        Assertions.assertEquals("admin", session.getUser());
        Assertions.assertEquals("Asia/Shanghai", session.getTimeZone());
        Assertions.assertEquals(42, session.getCatalogId());
        Assertions.assertEquals("my_catalog", session.getCatalogName());
    }

    @Test
    public void testDefaultValues() {
        ConnectorSession session = ConnectorSessionBuilder.create().build();

        Assertions.assertEquals("", session.getQueryId());
        Assertions.assertEquals("", session.getUser());
        Assertions.assertEquals("UTC", session.getTimeZone());
        Assertions.assertEquals("en_US", session.getLocale());
        Assertions.assertEquals("", session.getCatalogName());
    }
}
