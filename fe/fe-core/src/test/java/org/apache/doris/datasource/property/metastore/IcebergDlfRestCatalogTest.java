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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Disabled("set local DLF test credentials with -Ddoris.test.iceberg.dlf.access-key-id and "
        + "-Ddoris.test.iceberg.dlf.secret-access-key before running the test")
public class IcebergDlfRestCatalogTest {

    private static final String DLF_ACCESS_KEY_ID_PROPERTY = "doris.test.iceberg.dlf.access-key-id";
    private static final String DLF_SECRET_ACCESS_KEY_PROPERTY = "doris.test.iceberg.dlf.secret-access-key";
    private static final String DLF_REST_URI = "http://cn-beijing-vpc.dlf.aliyuncs.com/iceberg";
    private static final String DLF_WAREHOUSE = "new_dlf_iceberg_catalog";
    private static final String DLF_SIGNING_NAME = "DlfNext";
    private static final String DLF_SIGNING_REGION = "cn-beijing";

    private static final String ACCESS_KEY_ID = System.getProperty(
            DLF_ACCESS_KEY_ID_PROPERTY, "<your-access-key-id>");
    private static final String SECRET_ACCESS_KEY = System.getProperty(
            DLF_SECRET_ACCESS_KEY_PROPERTY, "<your-secret-access-key>");

    @Test
    public void testIcebergDlfRestCatalog() {
        Catalog dlfRestCatalog = initIcebergDlfRestCatalog();
        SupportsNamespaces nsCatalog = (SupportsNamespaces) dlfRestCatalog;
        // List namespaces and assert
        nsCatalog.listNamespaces(Namespace.empty()).forEach(namespace1 -> {
            System.out.println("Namespace: " + namespace1);
            Assertions.assertNotNull(namespace1, "Namespace should not be null");

            dlfRestCatalog.listTables(namespace1).forEach(tableIdentifier -> {
                System.out.println("Table: " + tableIdentifier.name());
                Assertions.assertNotNull(tableIdentifier, "TableIdentifier should not be null");

                // Load table history and assert
                Table iceTable = dlfRestCatalog.loadTable(tableIdentifier);
                iceTable.history().forEach(snapshot -> {
                    System.out.println("Snapshot: " + snapshot);
                    Assertions.assertNotNull(snapshot, "Snapshot should not be null");
                });

                CloseableIterable<FileScanTask> tasks = iceTable.newScan().planFiles();
                tasks.forEach(task -> {
                    System.out.println("FileScanTask: " + task);
                    Assertions.assertNotNull(task, "FileScanTask should not be null");
                });
            });
        });
    }

    private Catalog initIcebergDlfRestCatalog() {
        Map<String, String> options = Maps.newHashMap();
        options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        options.put(CatalogProperties.URI, DLF_REST_URI);
        options.put(CatalogProperties.WAREHOUSE_LOCATION, DLF_WAREHOUSE);
        // remove this endpoint prop, or, add https://
        // must set:
        // software.amazon.awssdk.core.exception.SdkClientException: Unable to load region from any of the providers in
        // the chain software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain@627ff1b8:
        // [software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider@67d32a54:
        // Unable to load region from system settings. Region must be specified either via environment variable
        // (AWS_REGION) or  system property (aws.region).,
        // software.amazon.awssdk.regions.providers.AwsProfileRegionProvider@2792b416: No region provided in profile:
        // default, software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider@5cff6b74:
        // Unable to contact EC2 metadata service.]
        // options.put(AwsClientProperties.CLIENT_REGION, "cn-beijing");
        // Forbidden: {"message":"Missing Authentication Token"}
        options.put("rest.sigv4-enabled", "true");
        // Forbidden: {"message":"Credential should be scoped to correct service: 'glue'. "}
        options.put("rest.signing-name", DLF_SIGNING_NAME);
        //  Forbidden: {"message":"The security token included in the request is invalid."}
        options.put("rest.access-key-id", ACCESS_KEY_ID);
        // Forbidden: {"message":"The request signature we calculated does not match the signature you provided.
        // Check your AWS Secret Access Key and signing method. Consult the service documentation for details."}
        options.put("rest.secret-access-key", SECRET_ACCESS_KEY);
        // same as AwsClientProperties.CLIENT_REGION, "ap-east-1"
        options.put("rest.signing-region", DLF_SIGNING_REGION);
        //options.put("rest.auth.type", "sigv4");

        // options.put("iceberg.catalog.warehouse", "<accountid>:s3tablescatalog/<table-bucket-name>");
        // 4. Build iceberg catalog
        Configuration conf = new Configuration();
        return CatalogUtil.buildIcebergCatalog("dlf_test", options, conf);
    }

    /**
     * CREATE CATALOG dlf PROPERTIES (
     *     'type' = 'iceberg',
     *     'warehouse' = 's3://warehouse',
     *     'iceberg.catalog.type' = 'rest',
     *     'iceberg.rest.uri' = '<dlf-rest-uri>',
     *     'oss.endpoint' = 'https://oss-cn-beijing.aliyuncs.com',
     *     'oss.access_key' = '<your-oss-access-key>',
     *     'oss.secret_key' = '<your-oss-secret-key>',
     *     'oss.region' = 'cn-beijing',
     *     'iceberg.rest.sigv4-enabled' = 'true',
     *     'iceberg.rest.signing-name' = 'DlfNext',
     *     'iceberg.rest.signing-region' = 'cn-beijing',
     *     'iceberg.rest.access-key-id' = '<your-access-key-id>',
     *     'iceberg.rest.secret-access-key' = '<your-secret-access-key>',
     *     'iceberg.rest.auth.type' = 'sigv4'
     * );
     */
}
