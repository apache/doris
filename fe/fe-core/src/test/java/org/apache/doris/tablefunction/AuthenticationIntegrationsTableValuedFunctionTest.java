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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TMetadataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class AuthenticationIntegrationsTableValuedFunctionTest {

    @Test
    public void testBasicMetadata() throws Exception {
        AuthenticationIntegrationsTableValuedFunction tvf =
                new AuthenticationIntegrationsTableValuedFunction(Collections.emptyMap());

        Assertions.assertEquals(TMetadataType.AUTHENTICATION_INTEGRATIONS, tvf.getMetadataType());

        List<Column> columns = tvf.getTableColumns();
        Assertions.assertEquals(5, columns.size());
        Assertions.assertEquals("IntegrationName", columns.get(0).getName());
        Assertions.assertEquals("Type", columns.get(1).getName());
        Assertions.assertEquals("Property", columns.get(2).getName());
        Assertions.assertEquals("Value", columns.get(3).getName());
        Assertions.assertEquals("Comment", columns.get(4).getName());

        Assertions.assertEquals(0,
                AuthenticationIntegrationsTableValuedFunction.getColumnIndexFromColumnName("integrationname"));
        Assertions.assertEquals(1,
                AuthenticationIntegrationsTableValuedFunction.getColumnIndexFromColumnName("type"));
        Assertions.assertEquals(2,
                AuthenticationIntegrationsTableValuedFunction.getColumnIndexFromColumnName("property"));
        Assertions.assertEquals(3,
                AuthenticationIntegrationsTableValuedFunction.getColumnIndexFromColumnName("value"));
        Assertions.assertEquals(4,
                AuthenticationIntegrationsTableValuedFunction.getColumnIndexFromColumnName("comment"));
    }

    @Test
    public void testRejectUnsupportedParams() {
        HashMap<String, String> params = new HashMap<>();
        params.put("k", "v");
        Assertions.assertThrows(AnalysisException.class,
                () -> new AuthenticationIntegrationsTableValuedFunction(params));
    }
}
