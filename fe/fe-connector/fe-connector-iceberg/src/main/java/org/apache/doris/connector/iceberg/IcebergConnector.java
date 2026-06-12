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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Iceberg connector implementation. Manages the lifecycle of an Iceberg SDK
 * {@link Catalog} instance for all metadata operations.
 *
 * <p>Supports all Iceberg catalog backends: REST, HMS, Glue, DLF, JDBC,
 * Hadoop, and S3Tables. The backend is determined by the {@code iceberg.catalog.type}
 * property, which maps to the appropriate {@code catalog-impl} class for the
 * Iceberg SDK's {@link CatalogUtil#buildIcebergCatalog}.</p>
 *
 * <p>Phase 1 provides read-only metadata operations (list databases, list tables,
 * get schema). Write operations, scan planning, actions (compaction, snapshot
 * management), and transaction support remain in fe-core temporarily.</p>
 */
public class IcebergConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog icebergCatalog;

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new IcebergConnectorMetadata(getOrCreateCatalog(), properties);
    }

    private Catalog getOrCreateCatalog() {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    icebergCatalog = createCatalog();
                }
            }
        }
        return icebergCatalog;
    }

    private Catalog createCatalog() {
        String catalogType = properties.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
        if (catalogType == null || catalogType.isEmpty()) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }

        Map<String, String> catalogProps = new HashMap<>(properties);
        String catalogImpl = resolveCatalogImpl(catalogType);
        catalogProps.put(CatalogProperties.CATALOG_IMPL, catalogImpl);
        // Iceberg SDK does not allow both "type" and "catalog-impl"
        catalogProps.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);

        Configuration conf = buildHadoopConf(catalogProps);
        String catalogName = context.getCatalogName();

        LOG.info("Creating Iceberg catalog '{}' with type='{}', impl='{}'",
                catalogName, catalogType, catalogImpl);

        return CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf);
    }

    /**
     * Resolve the Iceberg catalog implementation class from the catalog type string.
     */
    private static String resolveCatalogImpl(String catalogType) {
        switch (catalogType.toLowerCase()) {
            case "rest":
                return "org.apache.iceberg.rest.RESTCatalog";
            case "hms":
                return "org.apache.iceberg.hive.HiveCatalog";
            case "glue":
                return "org.apache.iceberg.aws.glue.GlueCatalog";
            case "hadoop":
                return "org.apache.iceberg.hadoop.HadoopCatalog";
            case "jdbc":
                return "org.apache.iceberg.jdbc.JdbcCatalog";
            case "s3tables":
                return "software.amazon.s3tables.iceberg.S3TablesCatalog";
            case "dlf":
                return "org.apache.doris.connector.iceberg.dlf.DLFCatalog";
            default:
                throw new DorisConnectorException(
                        "Unknown iceberg.catalog.type: " + catalogType
                                + ". Supported types: rest, hms, glue, hadoop, jdbc, s3tables, dlf");
        }
    }

    private static Configuration buildHadoopConf(Map<String, String> props) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")) {
                conf.set(key, entry.getValue());
            }
        }
        return conf;
    }

    @Override
    public void close() throws IOException {
        Catalog c = icebergCatalog;
        if (c != null) {
            if (c instanceof java.io.Closeable) {
                ((java.io.Closeable) c).close();
            }
            icebergCatalog = null;
        }
    }
}
