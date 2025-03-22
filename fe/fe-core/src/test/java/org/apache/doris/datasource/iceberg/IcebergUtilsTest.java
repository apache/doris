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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.hive.HiveCatalog;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

public class IcebergUtilsTest {
    @Test
    public void testParseTableName() {
        try {
            IcebergHMSExternalCatalog c1 =
                    new IcebergHMSExternalCatalog(1, "name", null, new HashMap<>(), "");
            HiveCatalog i1 = IcebergUtils.createIcebergHiveCatalog(c1, "i1");
            Assert.assertTrue(getListAllTables(i1));

            IcebergHMSExternalCatalog c2 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "true");
                                }},
                        "");
            HiveCatalog i2 = IcebergUtils.createIcebergHiveCatalog(c2, "i1");
            Assert.assertTrue(getListAllTables(i2));

            IcebergHMSExternalCatalog c3 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "false");
                                }},
                        "");
            HiveCatalog i3 = IcebergUtils.createIcebergHiveCatalog(c3, "i1");
            Assert.assertFalse(getListAllTables(i3));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private boolean getListAllTables(HiveCatalog hiveCatalog) throws IllegalAccessException, NoSuchFieldException {
        Field declaredField = hiveCatalog.getClass().getDeclaredField("listAllTables");
        declaredField.setAccessible(true);
        return declaredField.getBoolean(hiveCatalog);
    }
}
