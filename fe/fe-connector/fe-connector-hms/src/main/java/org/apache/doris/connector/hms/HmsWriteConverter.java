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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorColumn;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds Hive metastore API objects ({@link Table}, {@link Database}) from the connector-SPI write specs.
 *
 * <p>This is the SPI-clean equivalent of fe-core's {@code HiveUtil.toHiveTable}/{@code toHiveDatabase}
 * (plus the serde/table property split from {@code HiveProperties.setTableProperties}). It takes only
 * {@link HmsCreateTableRequest}/{@link HmsCreateDatabaseRequest} and connector-api types, with no fe-core
 * dependency. Doris&rarr;Hive type strings come from {@link HmsTypeMapping#toHiveTypeString}.</p>
 *
 * <p>Behavior is a faithful port of the legacy code: storage-descriptor / serde / input-output-format per
 * orc/parquet/text, per-format compression defaults, {@code MANAGED_TABLE} table type, and the
 * {@code "doris external hive table"} storage-descriptor tag.</p>
 */
public final class HmsWriteConverter {

    /** Table parameter key stamping which Doris build created the table (legacy {@code DORIS_VERSION}). */
    static final String DORIS_VERSION_KEY = "doris.version";
    /** User-facing compression property key consumed while building the table (removed afterwards). */
    static final String COMPRESSION_KEY = "compression";
    /** Fallback text compression when neither the request nor a session default supplies one. */
    static final String DEFAULT_TEXT_COMPRESSION = "plain";

    private static final Set<String> SUPPORTED_ORC_COMPRESSIONS =
            unmodifiableSet("plain", "zlib", "snappy", "zstd", "lz4");
    private static final Set<String> SUPPORTED_PARQUET_COMPRESSIONS =
            unmodifiableSet("plain", "snappy", "zstd", "lz4");
    private static final Set<String> SUPPORTED_TEXT_COMPRESSIONS =
            unmodifiableSet("plain", "gzip", "zstd", "bzip2", "lz4", "snappy");

    // Property keys that belong on the SerDe (not the table). Mirrors fe-core HiveProperties.HIVE_SERDE_PROPERTIES.
    private static final Set<String> HIVE_SERDE_PROPERTIES = unmodifiableSet(
            "field.delim",
            "colelction.delim",           // Hive 2 (legacy metastore typo, kept for parity)
            "collection.delim",           // Hive 3
            "separatorChar",              // OpenCSVSerde.SEPARATORCHAR
            "serialization.format",
            "line.delim",
            "quoteChar",                  // OpenCSVSerde.QUOTECHAR
            "mapkey.delim",
            "escape.delim",
            "escapeChar",                 // OpenCSVSerde.ESCAPECHAR
            "serialization.null.format",
            "skip.header.line.count",
            "skip.footer.line.count");

    private HmsWriteConverter() {
    }

    /**
     * Build a Hive metastore {@link Table} from a create-table request. Faithful port of legacy
     * {@code HiveUtil.toHiveTable}.
     */
    public static Table toHiveTable(HmsCreateTableRequest req) {
        Objects.requireNonNull(req.getDbName(), "Hive database name should be not null");
        Objects.requireNonNull(req.getTableName(), "Hive table name should be not null");
        Table table = new Table();
        table.setDbName(req.getDbName());
        table.setTableName(req.getTableName());
        // Legacy overflows the millis cast to int before multiplying; kept verbatim for byte-for-byte parity.
        int createTime = (int) System.currentTimeMillis() * 1000;
        table.setCreateTime(createTime);
        table.setLastAccessTime(createTime);
        Set<String> partitionSet = new HashSet<>(req.getPartitionKeys());
        SchemaSplit schema = toHiveSchema(req.getColumns(), partitionSet);

        table.setSd(toHiveStorageDesc(schema.dataColumns, req.getBucketCols(), req.getNumBuckets(),
                req.getFileFormat(), req.getLocation()));
        table.setPartitionKeys(schema.partitionColumns);

        table.setTableType("MANAGED_TABLE");
        Map<String, String> props = new HashMap<>(req.getProperties());
        // Legacy always stamps DORIS_VERSION; the plugin cannot compute the build version (no fe-core import),
        // so it is threaded on the request and stamped only when supplied. See HmsCreateTableRequest#getDorisVersion.
        if (req.getDorisVersion() != null && !req.getDorisVersion().isEmpty()) {
            props.put(DORIS_VERSION_KEY, req.getDorisVersion());
        }
        setCompressType(req, props);
        // set hive table comment by table properties
        props.put("comment", req.getComment());
        if (props.containsKey("owner")) {
            table.setOwner(props.get("owner"));
        }
        setTableProperties(table, props);
        return table;
    }

    /**
     * Build a Hive metastore {@link Database} from a create-database request. Faithful port of legacy
     * {@code HiveUtil.toHiveDatabase}.
     */
    public static Database toHiveDatabase(HmsCreateDatabaseRequest req) {
        Database database = new Database();
        database.setName(req.getDbName());
        String locationUri = req.getLocationUri();
        if (locationUri != null && !locationUri.isEmpty()) {
            database.setLocationUri(locationUri);
        }
        Map<String, String> props = new HashMap<>(req.getProperties());
        database.setParameters(props);
        database.setDescription(req.getComment());
        if (props.containsKey("owner")) {
            database.setOwnerName(props.get("owner"));
            database.setOwnerType(PrincipalType.USER);
        }
        return database;
    }

    private static void setCompressType(HmsCreateTableRequest req, Map<String, String> props) {
        String fileFormat = req.getFileFormat();
        String compression = props.get(COMPRESSION_KEY);
        // on HMS, default orc compression type is zlib and default parquet compression type is snappy.
        if (fileFormat.equalsIgnoreCase("parquet")) {
            if (isNotEmpty(compression) && !SUPPORTED_PARQUET_COMPRESSIONS.contains(compression)) {
                throw new IllegalArgumentException("Unsupported parquet compression type " + compression);
            }
            props.putIfAbsent("parquet.compression", isEmpty(compression) ? "snappy" : compression);
        } else if (fileFormat.equalsIgnoreCase("orc")) {
            if (isNotEmpty(compression) && !SUPPORTED_ORC_COMPRESSIONS.contains(compression)) {
                throw new IllegalArgumentException("Unsupported orc compression type " + compression);
            }
            props.putIfAbsent("orc.compress", isEmpty(compression) ? "zlib" : compression);
        } else if (fileFormat.equalsIgnoreCase("text")) {
            if (isNotEmpty(compression) && !SUPPORTED_TEXT_COMPRESSIONS.contains(compression)) {
                throw new IllegalArgumentException("Unsupported text compression type " + compression);
            }
            String textDefault = isEmpty(req.getDefaultTextCompression())
                    ? DEFAULT_TEXT_COMPRESSION : req.getDefaultTextCompression();
            props.putIfAbsent("text.compression", isEmpty(compression) ? textDefault : compression);
        } else {
            throw new IllegalArgumentException("Compression is not supported on " + fileFormat);
        }
        // remove if exists
        props.remove(COMPRESSION_KEY);
    }

    private static StorageDescriptor toHiveStorageDesc(List<FieldSchema> columns,
            List<String> bucketCols, int numBuckets, String fileFormat, String location) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(columns);
        setFileFormat(fileFormat, sd);
        if (location != null) {
            sd.setLocation(location);
        }
        sd.setBucketCols(bucketCols);
        sd.setNumBuckets(numBuckets);
        Map<String, String> parameters = new HashMap<>();
        parameters.put("tag", "doris external hive table");
        sd.setParameters(parameters);
        return sd;
    }

    private static void setFileFormat(String fileFormat, StorageDescriptor sd) {
        String inputFormat;
        String outputFormat;
        String serDe;
        if (fileFormat.equalsIgnoreCase("orc")) {
            inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            serDe = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
        } else if (fileFormat.equalsIgnoreCase("parquet")) {
            inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
            outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
            serDe = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        } else if (fileFormat.equalsIgnoreCase("text")) {
            inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
            outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
            serDe = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
        } else {
            throw new IllegalArgumentException("Creating table with an unsupported file format: " + fileFormat);
        }
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib(serDe);
        sd.setSerdeInfo(serDeInfo);
        sd.setInputFormat(inputFormat);
        sd.setOutputFormat(outputFormat);
    }

    private static SchemaSplit toHiveSchema(List<ConnectorColumn> columns, Set<String> partitionSet) {
        List<FieldSchema> hiveCols = new ArrayList<>();
        List<FieldSchema> hiveParts = new ArrayList<>();
        for (ConnectorColumn column : columns) {
            FieldSchema hiveFieldSchema = new FieldSchema();
            hiveFieldSchema.setType(HmsTypeMapping.toHiveTypeString(column.getType()));
            hiveFieldSchema.setName(column.getName());
            hiveFieldSchema.setComment(column.getComment());
            if (partitionSet.contains(column.getName())) {
                hiveParts.add(hiveFieldSchema);
            } else {
                hiveCols.add(hiveFieldSchema);
            }
        }
        return new SchemaSplit(hiveCols, hiveParts);
    }

    // Faithful port of fe-core HiveProperties.setTableProperties: serde keys land on the SerDe params,
    // everything else on the table params.
    private static void setTableProperties(Table table, Map<String, String> properties) {
        Map<String, String> serdeProps = new HashMap<>();
        Map<String, String> tblProps = new HashMap<>();
        for (String k : properties.keySet()) {
            if (HIVE_SERDE_PROPERTIES.contains(k)) {
                serdeProps.put(k, properties.get(k));
            } else {
                tblProps.put(k, properties.get(k));
            }
        }
        if (table.getParameters() == null) {
            table.setParameters(tblProps);
        } else {
            table.getParameters().putAll(tblProps);
        }
        if (table.getSd().getSerdeInfo().getParameters() == null) {
            table.getSd().getSerdeInfo().setParameters(serdeProps);
        } else {
            table.getSd().getSerdeInfo().getParameters().putAll(serdeProps);
        }
    }

    private static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    private static boolean isNotEmpty(String s) {
        return !isEmpty(s);
    }

    private static Set<String> unmodifiableSet(String... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }

    /** Data columns and partition-key columns split out of the full column list. */
    private static final class SchemaSplit {
        private final List<FieldSchema> dataColumns;
        private final List<FieldSchema> partitionColumns;

        private SchemaSplit(List<FieldSchema> dataColumns, List<FieldSchema> partitionColumns) {
            this.dataColumns = dataColumns;
            this.partitionColumns = partitionColumns;
        }
    }
}
