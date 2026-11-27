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

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

class IcebergCatalogInitializationSecurityTest {
    @Test
    void invalidPropertyTypeDoesNotDumpCatalogSecretsIntoExceptionChain() {
        Map<String, String> properties = new HashMap<>();
        properties.put("gcp.auth.credentials-json", "{\"private_key\":\"synthetic-private-material\"}");
        properties.put("unrelated.property", "synthetic-other-secret");
        CatalogProperty invalid = Mockito.mock(CatalogProperty.class);
        Mockito.when(invalid.getProperties()).thenReturn(properties);
        Mockito.when(invalid.getMetastoreProperties()).thenReturn(Mockito.mock(MetastoreProperties.class));
        IcebergRestExternalCatalog catalog = new IcebergRestExternalCatalog(1, "test", null, properties, "") {
            {
                catalogProperty = invalid;
            }
        };

        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, catalog::initCatalog);
        Assertions.assertInstanceOf(ClassCastException.class, failure.getCause());
        StringWriter output = new StringWriter();
        failure.printStackTrace(new PrintWriter(output));
        Assertions.assertFalse(output.toString().contains("synthetic-private-material"));
        Assertions.assertFalse(output.toString().contains("synthetic-other-secret"));
    }
}
