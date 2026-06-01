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

package org.apache.doris.foundation.property;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectorPropertiesUtilsTest {

    @Test
    void testBasicBinding() {
        Map<String, String> props = new HashMap<>();
        props.put("base.key", "base");
        props.put("string.key", "hello");
        props.put("int.key", "123");
        props.put("bool.key", "true");
        props.put("long.key", "456");
        props.put("double.key", "3.14");
        props.put("secret.key", "sensitive");

        SampleConfig config = new SampleConfig();
        ConnectorPropertiesUtils.bindConnectorProperties(config, props);

        Assertions.assertEquals("base", config.getBaseValue());
        Assertions.assertEquals("hello", config.getStringValue());
        Assertions.assertEquals(123, config.getIntValue());
        Assertions.assertTrue(config.isBoolValue());
        Assertions.assertEquals(456L, config.getLongValue());
        Assertions.assertEquals(3.14, config.getDoubleValue(), 0.0001);
        Assertions.assertEquals("sensitive", config.getSecretValue());
    }

    @Test
    void testAliasMatching() {
        Map<String, String> props = new HashMap<>();
        props.put("alias2", "matched-by-alias");

        SampleConfig config = new SampleConfig();
        ConnectorPropertiesUtils.bindConnectorProperties(config, props);

        Assertions.assertEquals("matched-by-alias", config.getAliasValue());
    }

    @Test
    void testUnsupportedFieldNotSet() {
        Map<String, String> props = new HashMap<>();
        props.put("glue.catalog_id", "should-not-set");

        SampleConfig config = new SampleConfig();
        ConnectorPropertiesUtils.bindConnectorProperties(config, props);

        Assertions.assertEquals("default", config.getGlueCatalogId());
    }

    @Test
    void testMissingFieldIgnored() {
        SampleConfig config = new SampleConfig();
        ConnectorPropertiesUtils.bindConnectorProperties(config, new HashMap<>());

        Assertions.assertNull(config.getStringValue());
        Assertions.assertEquals(0, config.getIntValue());
        Assertions.assertNull(config.getBaseValue());
    }

    @Test
    void testUnsupportedTypeThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("unsupported.key", "something");

        SampleConfig config = new SampleConfig();
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> ConnectorPropertiesUtils.bindConnectorProperties(config, props));

        Assertions.assertTrue(ex.getMessage().contains("Unsupported property type"));
    }

    @Test
    void testPartialBindingWithTypeErrorThrowsException() {
        Map<String, String> props = new HashMap<>();
        props.put("int.key", "not-a-number");

        SampleConfig config = new SampleConfig();
        Assertions.assertThrows(RuntimeException.class,
                () -> ConnectorPropertiesUtils.bindConnectorProperties(config, props));
    }

    @Test
    void testGetConnectorPropertiesIncludesSuperclassFields() {
        List<java.lang.reflect.Field> fields = ConnectorPropertiesUtils.getConnectorProperties(SampleConfig.class);
        Assertions.assertTrue(fields.stream().anyMatch(field -> field.getName().equals("baseValue")));
        Assertions.assertTrue(fields.stream().anyMatch(field -> field.getName().equals("stringValue")));
        Assertions.assertFalse(fields.stream().anyMatch(field -> field.getName().equals("glueCatalogId")));
    }

    @Test
    void testGetSensitiveKeys() {
        Set<String> keys = ConnectorPropertiesUtils.getSensitiveKeys(SampleConfig.class);
        Assertions.assertTrue(keys.contains("secret.key"));
        Assertions.assertTrue(keys.contains("secret.alias"));
        Assertions.assertFalse(keys.contains("string.key"));
    }

    static class BaseConfig {
        @ConnectorProperty(names = {"base.key"})
        private String baseValue;

        String getBaseValue() {
            return baseValue;
        }
    }

    static class SampleConfig extends BaseConfig {
        @ConnectorProperty(names = {"string.key"})
        private String stringValue;

        @ConnectorProperty(names = {"int.key"})
        private int intValue;

        @ConnectorProperty(names = {"bool.key"})
        private boolean boolValue;

        @ConnectorProperty(names = {"long.key"})
        private Long longValue;

        @ConnectorProperty(names = {"double.key"})
        private double doubleValue;

        @ConnectorProperty(names = {"alias1", "alias2"})
        private String aliasValue;

        @ConnectorProperty(names = {"glue.catalog_id"}, supported = false)
        private String glueCatalogId = "default";

        @ConnectorProperty(names = {"unsupported.key"})
        private UnsupportedFieldType unsupportedField;

        @ConnectorProperty(names = {"secret.key", "secret.alias"}, sensitive = true)
        private String secretValue;

        public String getStringValue() {
            return stringValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public boolean isBoolValue() {
            return boolValue;
        }

        public Long getLongValue() {
            return longValue;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public String getAliasValue() {
            return aliasValue;
        }

        public String getGlueCatalogId() {
            return glueCatalogId;
        }

        public UnsupportedFieldType getUnsupportedField() {
            return unsupportedField;
        }

        public String getSecretValue() {
            return secretValue;
        }
    }

    static class UnsupportedFieldType {
    }
}
