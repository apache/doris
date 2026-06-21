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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;

import java.util.Locale;
import java.util.Map;

/**
 * Pure, testable assembly core for the Iceberg connector flavor switch — the iceberg-SDK-specific bits
 * that stay in the connector. Mirrors the role of {@code PaimonCatalogFactory}: a stateless static
 * holder whose methods are PURE (they read only the supplied props — no env, no clock, no live
 * catalog), which is what makes them unit-testable offline.
 *
 * <p>P6.1 (this task) holds only the flavor resolution: {@link #resolveFlavor(Map)} (the lower-cased
 * {@code iceberg.catalog.type}) and {@link #resolveCatalogImpl(String)} (the catalog-impl class name
 * for the five {@code CatalogUtil}-built flavors plus the two bespoke ones). The full per-flavor
 * property / Hadoop-{@code Configuration} / {@code HiveConf} assembly currently dropped by the
 * skeleton — ported from the fe-core {@code AbstractIcebergProperties} + each
 * {@code Iceberg*MetaStoreProperties#initCatalog} — lands in a later task (P6-T05/T06/T07); this task
 * is a structural inversion only, with no behavior change.
 *
 * <p>Note: {@code s3tables} and {@code dlf} are listed here for completeness, but legacy does NOT build
 * them via {@code CatalogUtil.buildIcebergCatalog} (s3tables hand-builds an {@code S3TablesClient};
 * dlf uses {@code new DLFCatalog().setConf(..).initialize(..)}). Routing them through the impl-name
 * path is the existing skeleton behavior, preserved verbatim here; the bespoke instantiation fixes are
 * P6-T06 / P6-T07.
 */
public final class IcebergCatalogFactory {

    private IcebergCatalogFactory() {
    }

    /** Resolves the lower-cased flavor from {@code iceberg.catalog.type}; null/blank stays null. */
    public static String resolveFlavor(Map<String, String> props) {
        String catalogType = props.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
        if (catalogType == null || catalogType.isEmpty()) {
            return null;
        }
        return catalogType.toLowerCase(Locale.ROOT);
    }

    /**
     * Resolve the Iceberg catalog implementation class name from the catalog type string. PURE:
     * depends only on {@code catalogType}. Lifted verbatim from the former
     * {@code IcebergConnector.resolveCatalogImpl}.
     */
    public static String resolveCatalogImpl(String catalogType) {
        if (catalogType == null) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }
        switch (catalogType.toLowerCase(Locale.ROOT)) {
            case IcebergConnectorProperties.TYPE_REST:
                return "org.apache.iceberg.rest.RESTCatalog";
            case IcebergConnectorProperties.TYPE_HMS:
                return "org.apache.iceberg.hive.HiveCatalog";
            case IcebergConnectorProperties.TYPE_GLUE:
                return "org.apache.iceberg.aws.glue.GlueCatalog";
            case IcebergConnectorProperties.TYPE_HADOOP:
                return "org.apache.iceberg.hadoop.HadoopCatalog";
            case IcebergConnectorProperties.TYPE_JDBC:
                return "org.apache.iceberg.jdbc.JdbcCatalog";
            case IcebergConnectorProperties.TYPE_S3_TABLES:
                return "software.amazon.s3tables.iceberg.S3TablesCatalog";
            case IcebergConnectorProperties.TYPE_DLF:
                return "org.apache.doris.connector.iceberg.dlf.DLFCatalog";
            default:
                throw new DorisConnectorException(
                        "Unknown " + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + ": " + catalogType
                                + ". Supported types: rest, hms, glue, hadoop, jdbc, s3tables, dlf");
        }
    }
}
