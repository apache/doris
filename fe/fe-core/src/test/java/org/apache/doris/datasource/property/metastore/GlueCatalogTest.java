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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.UserException;

import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Disabled("Disabled until AWS credentials are available")
public class GlueCatalogTest {

    private GlueCatalog glueCatalog;
    private AWSGlueMetaStoreProperties glueProperties;
    private static final Namespace queryNameSpace = Namespace.of("test"); // Replace with your namespace
    private static final String AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"; // Replace with actual access key
    private static final String AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"; // Replace with actual secret key
    private static final String AWS_GLUE_ENDPOINT = "https://glue.ap-northeast-1.amazonaws.com"; // Replace with your endpoint

    @BeforeEach
    public void setUp() throws UserException {
        glueCatalog = new GlueCatalog();
        System.setProperty("queryNameSpace", "lakes_test_glue");

        // Setup properties
        Map<String, String> props = new HashMap<>();
        // Use environment variables for sensitive keys
        props.put("glue.access_key", AWS_ACCESS_KEY_ID);
        props.put("glue.secret_key", AWS_SECRET_ACCESS_KEY);
        props.put("glue.endpoint", AWS_GLUE_ENDPOINT);
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "glue");


        // Initialize AWSGlueProperties
        glueProperties = (AWSGlueMetaStoreProperties) AWSGlueMetaStoreProperties.create(props);

        // Convert to catalog properties
        Map<String, String> catalogProps = new HashMap<>();
        glueProperties.toIcebergGlueCatalogProperties(catalogProps);

        // Initialize Glue Catalog
        glueCatalog.initialize("ck", catalogProps);
    }

    @Test
    public void testListNamespaces() {

        // List namespaces and assert
        glueCatalog.listNamespaces(Namespace.empty()).forEach(namespace1 -> {
            System.out.println("Namespace: " + namespace1);
            Assertions.assertNotNull(namespace1, "Namespace should not be null");
        });
    }

    @Test
    public void testListTables() {
        // List tables in a given namespace
        glueCatalog.listTables(queryNameSpace).forEach(tableIdentifier -> {
            System.out.println("Table: " + tableIdentifier.name());
            Assertions.assertNotNull(tableIdentifier, "TableIdentifier should not be null");

            // Load table history and assert
            glueCatalog.loadTable(tableIdentifier).history().forEach(snapshot -> {
                System.out.println("Snapshot: " + snapshot);
                Assertions.assertNotNull(snapshot, "Snapshot should not be null");
            });
        });
    }

    @Test
    public void testConnection() {
        // Check if catalog can be initialized without errors
        Assertions.assertNotNull(glueCatalog, "Glue Catalog should be initialized");

        // Ensure at least one namespace exists
        Assertions.assertFalse(glueCatalog.listNamespaces(Namespace.empty()).isEmpty(),
                "Namespace list should not be empty");
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Close the Glue Catalog
        glueCatalog.close();
    }
}
