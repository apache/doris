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

package org.apache.doris.datasource;

import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class ExternalCatalogTest extends TestWithFeService {

    @Test
    public void testExternalCatalogAutoAnalyze() throws Exception {
        HMSExternalCatalog catalog = new HMSExternalCatalog();
        Assertions.assertFalse(catalog.enableAutoAnalyze());

        HashMap<String, String> prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "false");
        catalog.modifyCatalogProps(prop);
        Assertions.assertFalse(catalog.enableAutoAnalyze());

        prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "true");
        catalog.modifyCatalogProps(prop);
        Assertions.assertTrue(catalog.enableAutoAnalyze());

        prop = Maps.newHashMap();
        prop.put(ExternalCatalog.ENABLE_AUTO_ANALYZE, "TRUE");
        catalog.modifyCatalogProps(prop);
        Assertions.assertTrue(catalog.enableAutoAnalyze());
    }
}
