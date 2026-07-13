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

package org.apache.doris.datasource.hive;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.FileFormatConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HMSExternalCatalogParquetTimeZoneTest {
    private static Map<String, String> baseProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "hms");
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        return properties;
    }

    @Test
    public void testHiveParquetTimeZoneDefaultsToDisabled() throws Exception {
        HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, baseProperties(), "");
        Assertions.assertEquals("", catalog.getHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimeZoneAcceptsIanaZoneAndUtcOffset() throws Exception {
        for (String timezone : new String[] {
                "Asia/Shanghai", "+08:00", "UTC", "+14:00", "-12:00", "UTC+14:00", "GMT-12:00"
        }) {
            Map<String, String> properties = baseProperties();
            properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, timezone);
            HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
            catalog.checkProperties();
            Assertions.assertEquals(timezone, catalog.getHiveParquetTimeZone());
        }
    }

    @Test
    public void testHiveParquetTimeZoneCanonicalizesShorthandOffset() throws Exception {
        Map<String, String> properties = baseProperties();
        properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, "8:00");
        HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
        catalog.checkProperties();
        Assertions.assertEquals("+08:00", catalog.getHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimeZoneRejectsAmbiguousShortAliases() {
        for (String timezone : new String[] {"AET", "CST", "PRC"}) {
            Map<String, String> properties = baseProperties();
            properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, timezone);
            HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
            DdlException exception = Assertions.assertThrows(DdlException.class, catalog::checkProperties);
            Assertions.assertTrue(exception.getMessage().contains("short timezone aliases are not supported"));
        }
    }

    @Test
    public void testHiveParquetTimeZoneRejectsInvalidZone() {
        for (String timezone : new String[] {
                "Not/A_Timezone", "+15:00", "-13:00", "UTC+15:00", "GMT-13:00"
        }) {
            Map<String, String> properties = baseProperties();
            properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, timezone);
            HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
            DdlException exception = Assertions.assertThrows(DdlException.class, catalog::checkProperties);
            Assertions.assertTrue(exception.getMessage().contains(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE));
        }
    }
}
