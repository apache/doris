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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VariantTemplateDdlTest extends TestWithFeService {
    private boolean originalDecimal256;
    private boolean originalNereids;
    private boolean originalVariantV2;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_template_ddl");
        connectContext.setDatabase("variant_template_ddl");
    }

    @BeforeEach
    public void enableDecimal256() {
        originalDecimal256 = connectContext.getSessionVariable().enableDecimal256;
        originalNereids = connectContext.getState().isNereids();
        originalVariantV2 = Config.enable_variant_v2;
        connectContext.getSessionVariable().enableDecimal256 = true;
        connectContext.getState().setNereids(true);
    }

    @AfterEach
    public void restoreSettings() {
        connectContext.getSessionVariable().enableDecimal256 = originalDecimal256;
        connectContext.getState().setNereids(originalNereids);
        Config.enable_variant_v2 = originalVariantV2;
    }

    @Test
    public void testDecimal256TemplatesAreRejectedInV2() {
        Config.enable_variant_v2 = true;
        for (String type : new String[] {"DECIMAL(39, 2)", "ARRAY<DECIMAL(76, 2)>"}) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> createTable(ddl("rejected_template", "VARIANT<'amount':" + type + ">")));
            Assertions.assertTrue(error.getMessage().contains("Decimal256 is not supported in VARIANT templates"),
                    error.getMessage());
        }
    }

    @Test
    public void testDecimal128TemplateAndStandaloneDecimal256AreAllowed() throws Exception {
        Config.enable_variant_v2 = true;
        createTable(ddl("allowed_types",
                "VARIANT<'amount':DECIMAL(38, 2), 'items':ARRAY<DECIMAL(38, 2)>>, wide DECIMAL(76, 2)"));
    }

    @Test
    public void testDecimal256TemplatesAreAllowedInV1() throws Exception {
        Config.enable_variant_v2 = false;
        createTable(ddl("decimal256_v1",
                "VARIANT<'scalar':DECIMAL(39, 2), 'items':ARRAY<DECIMAL(76, 2)>>"));
    }

    private static String ddl(String table, String columns) {
        return "CREATE TABLE " + table + " (id INT, v " + columns + ") DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')";
    }
}
