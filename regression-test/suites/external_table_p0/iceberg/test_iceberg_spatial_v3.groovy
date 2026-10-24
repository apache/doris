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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request

suite("test_iceberg_spatial_v3", "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_spatial_v3"
    String dbName = "iceberg_spatial_v3_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def executeCommand = { String command, int timeoutSeconds = 300 ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        def process = new ProcessBuilder("/bin/bash", "-c", command).start()
        process.consumeProcessOutput(stdout, stderr)
        process.waitForOrKill(timeoutSeconds * 1000)
        assertEquals(0, process.exitValue(),
                "Command failed\nstdout:\n${stdout}\nstderr:\n${stderr}")
        return stdout.toString()
    }

    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    String sparkContainer = context.config.otherConfigs.get("icebergSparkContainer")
    if (sparkContainer == null || sparkContainer.isEmpty()) {
        String containers = executeCommand(
                "${dockerCommand} ps --format '{{.ID}}\t{{.Names}}'", 30)
        def matches = []
        containers.readLines().each { String line ->
            String containerId = line.split(/\t/, 2)[0]
            String probe = "${dockerCommand} exec ${containerId} bash -lc " +
                    "'test -f /mnt/SUCCESS && command -v spark-sql >/dev/null'"
            try {
                executeCommand(probe, 30)
                matches.add(containerId)
            } catch (Throwable ignored) {
                // Only the Spark service has the Iceberg API used to create this V3 schema.
            }
        }
        assertEquals(1, matches.size(), "Expected exactly one usable Spark Iceberg container")
        sparkContainer = matches[0]
    }

    String javaSource = '''
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class CreateIcebergSpatialV3Table {
    private static void appendParquetRow(Table table, MessageType message, Group row) throws Exception {
        String localFile = "/tmp/spatial-fixture-" + UUID.randomUUID() + ".parquet";
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(localFile)).withType(message).build()) {
            writer.write(row);
        }
        OutputFile output = table.io().newOutputFile(table.location()
                + "/data/spatial-fixture-" + UUID.randomUUID() + ".parquet");
        long size = Files.size(Paths.get(localFile));
        try (OutputStream stream = output.create()) {
            Files.copy(Paths.get(localFile), stream);
        }
        Files.delete(Paths.get(localFile));
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(output.location()).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(size).withRecordCount(1).build()).commit();
    }

    private static String parquetField(Types.NestedField field) {
        String head = (field.isRequired() ? "required " : "optional ");
        String tail = " = " + field.fieldId();
        switch (field.type().typeId()) {
            case INTEGER: return head + "int32 " + field.name() + tail + "; ";
            case STRING: return head + "binary " + field.name() + " (UTF8)" + tail + "; ";
            case GEOMETRY:
            case GEOGRAPHY: return head + "binary " + field.name() + tail + "; ";
            case LIST: return head + "group " + field.name() + " (LIST)" + tail
                    + " { repeated group list { " + parquetField(field.type().asListType().fields().get(0)) + "} } ";
            case MAP: return head + "group " + field.name() + " (MAP)" + tail
                    + " { repeated group key_value { " + parquetField(field.type().asMapType().fields().get(0))
                    + parquetField(field.type().asMapType().fields().get(1)) + "} } ";
            case STRUCT:
                StringBuilder structure = new StringBuilder(head + "group " + field.name() + tail + " { ");
                for (Types.NestedField child : field.type().asStructType().fields()) {
                    structure.append(parquetField(child));
                }
                return structure.append("} ").toString();
            default: throw new IllegalArgumentException("Unexpected fixture type: " + field.type());
        }
    }

    private static Binary binary(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return Binary.fromConstantByteArray(bytes);
    }

    private static void createNestedSource(Catalog catalog, Namespace namespace,
            Map<String, String> properties) throws Exception {
        java.util.List<Types.NestedField> fields = new java.util.ArrayList<>();
        fields.add(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        int nextId = 2;
        for (boolean geography : new boolean[] {false, true}) {
            String prefix = geography ? "gg_" : "g_";
            org.apache.iceberg.types.Type geo = geography ? Types.GeographyType.crs84() : Types.GeometryType.crs84();
            fields.add(Types.NestedField.optional(nextId++, prefix + "array",
                    Types.ListType.ofOptional(nextId++, geo)));
            fields.add(Types.NestedField.optional(nextId++, prefix + "map",
                    Types.MapType.ofOptional(nextId++, nextId++, Types.StringType.get(), geo)));
            fields.add(Types.NestedField.optional(nextId++, prefix + "struct", Types.StructType.of(
                    Types.NestedField.optional(nextId++, "shape", geo),
                    Types.NestedField.optional(nextId++, "label", Types.StringType.get()))));
            fields.add(Types.NestedField.optional(nextId++, prefix + "deep",
                    Types.MapType.ofOptional(nextId++, nextId++, Types.StringType.get(),
                            Types.ListType.ofOptional(nextId++, Types.StructType.of(
                                    Types.NestedField.optional(nextId++, "shape", geo))))));
        }
        TableIdentifier name = TableIdentifier.of(namespace, "spatial_nested_source");
        if (catalog.tableExists(name)) {
            catalog.dropTable(name);
        }
        Table table = catalog.createTable(name, new Schema(fields), PartitionSpec.unpartitioned(), properties);
        StringBuilder schema = new StringBuilder("message nested_spatial { ");
        for (Types.NestedField field : table.schema().columns()) {
            schema.append(parquetField(field));
        }
        MessageType message = MessageTypeParser.parseMessageType(schema.append("}").toString());
        String[] payloads = {"0101000000000000000000F03F0000000000000040",
                "00000000013FF00000000000004000000000000000",
                "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
                "010200000000000000"};
        for (int id = 1; id <= 6; id++) {
            Group row = new SimpleGroupFactory(message).newGroup().append("id", id);
            for (String prefix : new String[] {"g_", "gg_"}) {
                if (id == 5) {
                    continue; // SQL NULL parents.
                }
                Group array = row.addGroup(prefix + "array");
                Group map = row.addGroup(prefix + "map");
                Group structure = row.addGroup(prefix + "struct").append("label", "row" + id);
                Group deep = row.addGroup(prefix + "deep");
                if (id == 6) {
                    continue; // Empty containers and a present struct with a NULL spatial field.
                }
                Binary point = binary(payloads[id - 1]);
                array.addGroup("list").append("element", point);
                array.addGroup("list");
                map.addGroup("key_value").append("key", "shape").append("value", point);
                map.addGroup("key_value").append("key", "absent");
                structure.append("shape", point);
                Group list = deep.addGroup("key_value").append("key", "shapes").addGroup("value");
                list.addGroup("list").addGroup("element").append("shape", point);
                list.addGroup("list").addGroup("element");
                list.addGroup("list");
            }
            appendParquetRow(table, message, row);
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "rest");
        properties.put("uri", "http://rest:8181");
        properties.put("warehouse", "s3://warehouse/wh/");
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");
        Catalog catalog = CatalogUtil.buildIcebergCatalog("demo", properties, null);
        SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
        Namespace namespace = Namespace.of(args[0]);
        if (!namespaceCatalog.namespaceExists(namespace)) {
            namespaceCatalog.createNamespace(namespace);
        }
        TableIdentifier identifier = TableIdentifier.of(namespace, args[1]);
        if (catalog.tableExists(identifier)) {
            catalog.dropTable(identifier);
        }
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()),
                Types.NestedField.optional(3, "geog", Types.GeographyType.crs84()));
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "3");
        tableProperties.put("write.format.default", "parquet");
        catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(), tableProperties);
        TableIdentifier nested = TableIdentifier.of(namespace, "nested_spatial_values");
        if (catalog.tableExists(nested)) {
            catalog.dropTable(nested);
        }
        Table nestedTable = catalog.createTable(nested, new Schema(
                Types.NestedField.optional(1, "shapes",
                        Types.ListType.ofOptional(2, Types.GeometryType.crs84()))),
                PartitionSpec.unpartitioned(), tableProperties);
        nestedTable.updateProperties().set("write.merge.mode", "merge-on-read").commit();
        TableIdentifier projected = TableIdentifier.of(namespace, "projected_values");
        if (catalog.tableExists(projected)) {
            catalog.dropTable(projected);
        }
        Table projectedTable = catalog.createTable(projected, new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "geom", Types.GeometryType.of("EPSG:3857")),
                Types.NestedField.optional(3, "geog", Types.GeographyType.of("OGC:CRS84",
                        org.apache.iceberg.types.EdgeAlgorithm.VINCENTY))),
                PartitionSpec.unpartitioned(), tableProperties);
        // A table property resolves the Iceberg CRS reference; it is unavailable to the BE file writer.
        Map<String, String> referencedProperties = new HashMap<>(tableProperties);
        referencedProperties.put("my_crs", "{\\"type\\":\\"GeographicCRS\\",\\"name\\":\\"WGS 84\\",\\"datum\\":{\\"type\\":\\"GeodeticReferenceFrame\\",\\"name\\":\\"World Geodetic System 1984\\",\\"ellipsoid\\":{\\"name\\":\\"WGS 84\\",\\"semi_major_axis\\":6378137,\\"inverse_flattening\\":298.257223563}},\\"coordinate_system\\":{\\"subtype\\":\\"ellipsoidal\\",\\"axis\\":[{\\"name\\":\\"Geodetic longitude\\",\\"abbreviation\\":\\"Lon\\",\\"direction\\":\\"east\\",\\"unit\\":\\"degree\\"},{\\"name\\":\\"Geodetic latitude\\",\\"abbreviation\\":\\"Lat\\",\\"direction\\":\\"north\\",\\"unit\\":\\"degree\\"}]}}");
        TableIdentifier referenced = TableIdentifier.of(namespace, "referenced_crs_values");
        if (catalog.tableExists(referenced)) {
            catalog.dropTable(referenced);
        }
        Table referencedTable = catalog.createTable(referenced, new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "geom", Types.GeometryType.of("projjson:my_crs")),
                Types.NestedField.optional(3, "geog", Types.GeographyType.of("projjson:my_crs",
                        org.apache.iceberg.types.EdgeAlgorithm.SPHERICAL))),
                PartitionSpec.unpartitioned(), referencedProperties);
        for (boolean isGeography : new boolean[] {false, true}) {
            String column = isGeography ? "geog" : "geom";
            Schema copySchema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, column, isGeography
                            ? Types.GeographyType.of("projjson:my_crs",
                                    org.apache.iceberg.types.EdgeAlgorithm.SPHERICAL)
                            : Types.GeometryType.of("projjson:my_crs")));
            for (boolean partitioned : new boolean[] {false, true}) {
                String name = (isGeography ? "referenced_geography_copy" : "referenced_geometry_copy")
                        + (partitioned ? "_partitioned" : "");
                TableIdentifier copy = TableIdentifier.of(namespace, name);
                if (catalog.tableExists(copy)) {
                    catalog.dropTable(copy);
                }
                Table copyTable = catalog.createTable(copy, copySchema, partitioned
                        ? PartitionSpec.builderFor(copySchema).identity("id").build()
                        : PartitionSpec.unpartitioned(), referencedProperties);
                System.out.println("SPATIAL_DATA_LOCATION\\t" + name + "\\t"
                        + copyTable.properties().getOrDefault("write.data.path", copyTable.location() + "/data"));
            }
        }
        // Iceberg 1.10 exposes spatial schema types but its generic Parquet writer does not implement them.
        // Store standard WKB BYTE_ARRAY values with field ids; the Iceberg schema carries CRS/algorithm.
        String parquetSchema = "message spatial { required int32 id = "
                + projectedTable.schema().findField("id").fieldId() + "; optional binary geom = "
                + projectedTable.schema().findField("geom").fieldId() + "; optional binary geog = "
                + projectedTable.schema().findField("geog").fieldId() + "; }";
        MessageType message = MessageTypeParser.parseMessageType(parquetSchema);
        Group row = new SimpleGroupFactory(message).newGroup().append("id", 1);
        ByteBuffer point = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        point.put((byte) 1).putInt(1).putDouble(1000).putDouble(2000).flip();
        row.append("geom", Binary.fromConstantByteBuffer(point));
        ByteBuffer geography = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        geography.put((byte) 1).putInt(1).putDouble(1).putDouble(2).flip();
        row.append("geog", Binary.fromConstantByteBuffer(geography));
        appendParquetRow(projectedTable, message, row);
        Group referencedRow = new SimpleGroupFactory(message).newGroup().append("id", 1);
        referencedRow.append("geom", row.getBinary("geog", 0));
        referencedRow.append("geog", row.getBinary("geog", 0));
        appendParquetRow(referencedTable, message, referencedRow);
        MessageType nestedMessage = MessageTypeParser.parseMessageType(
                "message spatial_nested { optional group shapes (LIST) = 1 { "
                + "repeated group list { optional binary element = 2; } } }");
        Group nestedRow = new SimpleGroupFactory(nestedMessage).newGroup();
        nestedRow.addGroup("shapes").addGroup("list").append("element", row.getBinary("geog", 0));
        appendParquetRow(nestedTable, nestedMessage, nestedRow);
        createNestedSource(catalog, namespace, tableProperties);
    }
}
'''
    String encodedJavaSource = javaSource.getBytes("UTF-8").encodeBase64().toString()
    String fixtureOutput = executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '"
            + "task_java_dir=\$(mktemp -d /tmp/iceberg-spatial-v3.XXXXXX) && "
            + "echo ${encodedJavaSource} | base64 -d >\"\$task_java_dir/CreateIcebergSpatialV3Table.java\" && "
            + "javac -cp \"/opt/spark/jars/*\" \"\$task_java_dir/CreateIcebergSpatialV3Table.java\" && "
            + "java -cp \"\$task_java_dir:/opt/spark/jars/*\" "
            + "CreateIcebergSpatialV3Table ${dbName} geometry_values'")

    def dataLocations = [:]
    fixtureOutput.readLines().findAll { it.startsWith("SPATIAL_DATA_LOCATION\t") }.each { line ->
        def fields = line.split("\t", 3)
        dataLocations[fields[1]] = fields[2]
    }
    def minioClient = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration("http://${externalEnvIp}:${minioPort}", "us-east-1"))
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password")))
            .build()
    def listDataObjects = { String table ->
        String location = dataLocations[table]
        assertNotNull(location, "Missing data location for ${table}")
        def uri = new URI(location)
        def objects = new TreeMap<String, Long>()
        String token = null
        while (true) {
            def listing = minioClient.listObjectsV2(new ListObjectsV2Request()
                    .withBucketName(uri.host).withPrefix(uri.path.substring(1).replaceAll('/+$', '') + '/')
                    .withContinuationToken(token))
            listing.objectSummaries.each { objects[it.key] = it.size }
            if (!listing.isTruncated()) {
                break
            }
            token = listing.nextContinuationToken
        }
        return objects
    }

    def createCatalog = {
        sql """drop catalog if exists ${catalogName}"""
        sql """
            create catalog ${catalogName} properties (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'rest',
                'uri' = 'http://${externalEnvIp}:${restPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.region' = 'us-east-1'
            )
        """
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
    }
    createCatalog()
    test {
        sql """create table spatial_invalid_geometry_crs (geom geometry(''))
            engine=iceberg properties ('format-version' = '3')"""
        exception "Spatial CRS must not be empty"
    }
    test {
        sql """create table spatial_invalid_geography_crs (geog geography('', 'spherical'))
            engine=iceberg properties ('format-version' = '3')"""
        exception "Spatial CRS must not be empty"
    }
    test {
        sql """create table spatial_invalid_algorithm (geog geography('OGC:CRS84', 'unknown'))
            engine=iceberg properties ('format-version' = '3')"""
        exception "Unsupported Geography algorithm"
    }
    sql """
        insert into geometry_values values
            (1, ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'),
                ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')),
            (2, NULL, NULL),
            (3, ST_GeometryFromWKBTyped('010100000000000000000008400000000000001040'), NULL),
            (4, NULL, ST_GeogFromWKB('010100000000000000000008400000000000001040'))
    """
    // Doris writes OPTIONAL GEOMETRY/GEOGRAPHY annotations; count each leaf separately
    // to exercise definition-level COUNT pushdown, including SQL NULL rows.
    for (boolean scanV2 : [false, true]) {
        for (boolean pushdown : [false, true]) {
            "qt_optional_geometry_count_${scanV2}_${pushdown}"("""select /*+ SET_VAR(
                enable_file_scanner_v2=${scanV2}, enable_push_down_no_group_agg=${pushdown}) */
                count(geom) from geometry_values""")
            "qt_optional_geography_count_${scanV2}_${pushdown}"("""select /*+ SET_VAR(
                enable_file_scanner_v2=${scanV2}, enable_push_down_no_group_agg=${pushdown}) */
                count(geog) from geometry_values""")
        }
    }
    qt_spatial_schema """desc geometry_values"""
    // These factories do not implement scalar spatial values. Fail in FE, including
    // analytic DISTINCT, which is rewritten to multi_distinct_count after binding.
    for (String column : ["geom", "geog"]) {
        for (String query : [
                "select ${column}, count(*) from geometry_values group by ${column}",
                "select id, ${column}, count(*) from geometry_values group by id, ${column}",
                "select ${column}, count(*) from geometry_values group by grouping sets ((${column}), ())",
                "select distinct ${column} from geometry_values",
                "select distinct id, ${column} from geometry_values"]) {
            test {
                sql query
                exception "GROUP BY and DISTINCT do not support spatial keys"
            }
        }
        for (String expression : ["min(${column})", "max(${column})",
                "min(${column}) over ()", "max(${column}) over ()",
                "count(distinct ${column})", "count(distinct id, ${column})"]) {
            test {
                sql "select ${expression} from geometry_values"
                exception "does not support spatial arguments"
            }
        }
        // Compare only implemented types; MAP_CONTAINS_KEY must not inspect spatial values.
        for (String expression : [
                "map_contains_value(map(id, ${column}), ${column})",
                "map_contains_value(map(id, ${column}), NULL)",
                "map_contains_value(map(id, id), ${column})",
                "map_contains_value(map(id, array(${column})), array(${column}))",
                "map_contains_key(map(${column}, id), ${column})",
                "map_contains_key(map(${column}, id), NULL)",
                "map_contains_key(map(id, ${column}), ${column})",
                "map_contains_entry(map(id, ${column}), id, ${column})",
                "map_contains_entry(map(id, ${column}), id, NULL)",
                "map_contains_entry(map(id, id), id, ${column})",
                "map_contains_entry(map(id, id), ${column}, id)",
                "map_contains_entry(map(${column}, id), NULL, id)",
                "map_contains_entry(map(id, array(${column})), id, array(${column}))"]) {
            test {
                sql """select ${expression} from geometry_values"""
                exception "does not support spatial comparison"
            }
        }
        "order_qt_spatial_map_key_lookup_${column}"("""select id,
            map_contains_key(map(id, ${column}), id),
            map_contains_key(map(id, ${column}), id + 1),
            map_contains_key(map(id, ${column}), CAST(NULL AS INT)),
            hex(ST_AsBinary(element_at(map(id, ${column}), id))) from geometry_values""")
        // Distinct imported slots must fail in FE, before BE's unsupported scalar dispatch.
        for (String function : ["greatest", "least"]) {
            for (String arguments : ["l.${column}, r.${column}", "NULL, r.${column}",
                    "l.${column}, NULL", "l.id, r.${column}"]) {
                test {
                    sql """select ${function}(${arguments}) from geometry_values l
                        cross join geometry_values r"""
                    exception "${function} does not support spatial arguments"
                }
            }
        }
        for (String function : ["min_by", "max_by"]) {
            for (String arguments : ["${column}, id", "id, ${column}"]) {
                test {
                    sql """select ${function}(${arguments}) from geometry_values"""
                    exception "${function} does not support spatial arguments"
                }
            }
        }
        for (String arguments : ["${column}, 2", "${column}, 2, 4", "array(${column}), 2"]) {
            test {
                sql """select topn_array(${arguments}) from geometry_values"""
                exception "topn_array does not support spatial arguments"
            }
        }
        for (String expression : ["multi_distinct_count(${column})", "count(distinct ${column}) over ()"]) {
            test {
                sql """select ${expression} from geometry_values"""
                exception "does not support spatial arguments"
            }
        }
        for (String function : ["group_array_union", "group_array_intersect"]) {
            test {
                sql """select ${function}(array(${column})) from geometry_values"""
                exception "${function} does not support spatial arguments"
            }
        }
        for (String suffix : ["", "_state"]) {
            for (String arguments : ["${column}, 1.0", "${column}, 1.0, 0.0"]) {
                test {
                    sql """select linear_histogram${suffix}(${arguments}) from geometry_values"""
                    exception "linear_histogram does not support spatial arguments"
                }
            }
            for (String function : ["map_agg", "map_agg_v2"]) {
                test {
                    sql """select ${function}${suffix}(${column}, id) from geometry_values"""
                    exception "map_agg does not support spatial keys"
                }
            }
            for (String function : ["count_map", "sum_map", "avg_map", "min_map", "max_map"]) {
                test {
                    sql """select ${function}${suffix}(map(${column}, id)) from geometry_values"""
                    exception "${function} does not support spatial MAP keys"
                }
            }
        }
        // Ordinary keys retain raw nullable WKB values through MAP_AGG and its alias.
        "order_qt_spatial_map_agg_values_${column}"("""select id,
            hex(ST_AsBinary(element_at(primary_values, id))), hex(ST_AsBinary(element_at(alias_values, id)))
            from geometry_values cross join
                (select map_agg(id, ${column}) primary_values, map_agg_v2(id, ${column}) alias_values
                 from geometry_values) aggregates""")
        // Repeated ordinary keys exercise value updates and nullable aggregate states.
        "qt_spatial_map_combinator_values_${column}"("""select element_at(counts, 1),
            hex(ST_AsBinary(element_at(minimums, 1))), hex(ST_AsBinary(element_at(maximums, 1)))
            from (select count_map(map(1, ${column})) counts, min_map(map(1, ${column})) minimums,
                max_map(map(1, ${column})) maximums from geometry_values) aggregates""")
        "qt_spatial_map_state_values_${column}"("""select /*+ SET_VAR(enable_agg_state=true) */
            hex(ST_AsBinary(element_at(map_agg_merge(primary_state), 1))),
            hex(ST_AsBinary(element_at(map_agg_v2_merge(alias_state), 1))),
            element_at(count_map_merge(count_state), 1),
            hex(ST_AsBinary(element_at(min_map_merge(min_state), 1))),
            hex(ST_AsBinary(element_at(max_map_merge(max_state), 1)))
            from (select map_agg_state(id, ${column}) primary_state, map_agg_v2_state(id, ${column}) alias_state,
                count_map_state(map(1, ${column})) count_state, min_map_state(map(1, ${column})) min_state,
                max_map_state(map(1, ${column})) max_state from geometry_values) states""")
    }
    // A typed NULL has no aggregate producer: validate its state descriptor before BE initialization.
    for (String spatial : ["GEOMETRY", "GEOGRAPHY"]) {
        for (String function : ["count_map", "sum_map", "avg_map", "min_map", "max_map"]) {
            String stateType = "AGG_STATE<${function}(MAP<${spatial}, INT>)>"
            String state = "CAST(NULL AS ${stateType})"
            for (String query : [
                    "select ${state} from numbers('number'='3')",
                    "select TRY_CAST(NULL AS ${stateType}) from numbers('number'='3')",
                    "select CONVERT(NULL, ${stateType}) from numbers('number'='3')",
                    "select s from (select ${state} s from numbers('number'='3')) states",
                    "select ${function}_merge(s) from (select ${state} s from numbers('number'='3')) states"]) {
                test {
                    sql query.replaceFirst("select", "select /*+ SET_VAR(enable_agg_state=true) */")
                    exception "${function} does not support spatial MAP keys"
                }
            }
        }
        for (String signature : ["min(${spatial})", "max(${spatial})",
                "map_agg(${spatial}, INT)", "map_agg_v2(${spatial}, INT)",
                "linear_histogram(${spatial}, DOUBLE)", "linear_histogram(${spatial}, DOUBLE, DOUBLE)"]) {
            test {
                sql """select /*+ SET_VAR(enable_agg_state=true) */ CAST(NULL AS AGG_STATE<${signature}>)
                    from numbers('number'='3')"""
                exception "does not support spatial"
            }
        }
        List<String> supportedStates = ["map_agg(INT, ${spatial})", "map_agg_v2(INT, ${spatial})",
                "count_map(MAP<INT, ${spatial}>)", "min_map(MAP<INT, ${spatial}>)",
                "max_map(MAP<INT, ${spatial}>)", "any_value(${spatial})",
                "count(${spatial})", "array_agg(${spatial})", "collect_list(${spatial})",
                "collect_list(${spatial}, INT)"]
        String projections = supportedStates.collect { "CAST(NULL AS AGG_STATE<${it}>)" }.join(", ")
        "order_qt_spatial_declared_state_nulls_${spatial.toLowerCase()}"("""select
            /*+ SET_VAR(enable_agg_state=true) */ number, ${projections}
            from numbers('number'='3') order by number""")
    }
    order_qt_ordinary_declared_state_nulls """select /*+ SET_VAR(enable_agg_state=true) */ number,
        CAST(NULL AS AGG_STATE<linear_histogram(INT, DOUBLE)>), CAST(NULL AS AGG_STATE<map_agg(INT, INT)>),
        CAST(NULL AS AGG_STATE<count_map(MAP<INT, INT>)>), CAST(NULL AS AGG_STATE<sum_map(MAP<INT, INT>)>),
        CAST(NULL AS AGG_STATE<avg_map(MAP<INT, INT>)>), CAST(NULL AS AGG_STATE<min_map(MAP<INT, INT>)>),
        CAST(NULL AS AGG_STATE<max_map(MAP<INT, INT>)>) from numbers('number'='3') order by number"""
    // Imported nested columns must obey the same group-array admission rule.
    for (String function : ["group_array_union", "group_array_intersect"]) {
        test {
            sql """select ${function}(shapes) from nested_spatial_values"""
            exception "${function} does not support spatial arguments"
        }
    }
    qt_ordinary_parallel_aggregates """select min_by(id, id), max_by(id, id),
        array_sort(topn_array(if(id > 1, 1, 2), 2)),
        array_sort(group_array_union(array(id))),
        array_sort(group_array_intersect(array(id, 1))), multi_distinct_count(id)
        from geometry_values"""
    order_qt_ordinary_analytic_distinct """select id, count(distinct id) over ()
        from geometry_values"""
    qt_ordinary_histogram_and_map_aggregates """select linear_histogram(id, 1.0),
        linear_histogram(id, 1.0, 0.0), element_at(map_agg(id, id), 1),
        element_at(count_map(map(1, id)), 1), element_at(sum_map(map(1, id)), 1),
        element_at(avg_map(map(1, id)), 1), element_at(min_map(map(1, id)), 1),
        element_at(max_map(map(1, id)), 1) from geometry_values"""
    order_qt_ordinary_map_contains """select id,
        map_contains_key(map(id, id), id), map_contains_value(map(id, id), id),
        map_contains_entry(map(id, id), id, id), map_contains_value(map(id, id), id + 1),
        map_contains_entry(map(id, id), id, id + 1) from geometry_values"""
    for (String spatialColumn : ["geom", "geog"]) {
        "qt_spatial_multikey_${spatialColumn}"("""select id from geometry_values
            order by ${spatialColumn} asc nulls last, id""")
        "qt_spatial_multikey_topn_${spatialColumn}"("""select id from geometry_values
            order by ${spatialColumn} desc nulls first, id limit 3""")
        "qt_spatial_multikey_nonnull_${spatialColumn}"("""select id from geometry_values
            where ${spatialColumn} is not null order by ${spatialColumn}, id""")
    }
    order_qt_ordinary_greatest_least """select id, greatest(id, id + 1), least(id, id + 1),
        greatest(id, if(id = 1, NULL, id + 1)), least(id, if(id = 1, NULL, id + 1))
        from geometry_values"""
    // All pairs below are real nullable Iceberg columns, including line/polygon boundaries.
    String meridian = "0102000000020000000000000000002440000000000080464000000000000024400000000000004740"
    String overlap = "01020000000200000000000000000024406666666666a6464000000000000024403333333333f34640"
    String reverseOverlap = "01020000000200000000000000000024403333333333f3464000000000000024406666666666a64640"
    String nearby = "010200000002000000e7db0000000024406666666666a64640e7db0000000024403333333333f34640"
    String outside = "0102000000020000001924ffffffff23406666666666a646401924ffffffff23403333333333f34640"
    String beyond = "0102000000020000000000000000002440cdcccccccc0c474000000000000024409a99999999194740"
    String multiEdge = "010200000003000000000000000000224000000000000046400000000000002440000000000080464000000000000024400000000000004740"
    String square = "010300000001000000050000000000000000002440000000000080464000000000000026400000000000804640000000000000264000000000000047400000000000002440000000000000474000000000000024400000000000804640"
    String neighbor = "0103000000010000000500000000000000000024406666666666a6464000000000000024403333333333f3464000000000000022403333333333f3464000000000000022406666666666a6464000000000000024406666666666a64640"
    String nearbyPolygon = "010300000001000000050000001924ffffffff23406666666666a646401924ffffffff23403333333333f3464000000000000022403333333333f3464000000000000022406666666666a646401924ffffffff23406666666666a64640"
    sql """drop table if exists spatial_relation_values"""
    sql """create table spatial_relation_values (id int, lhs geography, rhs geography)
        engine=iceberg properties ('format-version' = '3')"""
    List<String> relationRows = []
    [[meridian, overlap], [meridian, reverseOverlap], [meridian, nearby], [meridian, beyond],
     [multiEdge, overlap], [square, overlap], [square, nearby], [square, outside],
     [square, neighbor], [square, nearbyPolygon]].eachWithIndex { pair, index ->
        relationRows.add("(${index}, ST_GeogFromWKB('${pair[0]}'), ST_GeogFromWKB('${pair[1]}'))")
    }
    relationRows.add("(10, NULL, ST_GeogFromWKB('${overlap}'))")
    relationRows.add("(11, ST_GeogFromWKB('${meridian}'), NULL)")
    relationRows.add("(12, NULL, NULL)")
    sql """insert into spatial_relation_values values ${relationRows.join(',')}"""
    order_qt_spatial_nonpoint_relations """select id, ST_Intersects(lhs, rhs), ST_Intersects(rhs, lhs),
        ST_Disjoint(lhs, rhs), ST_Disjoint(rhs, lhs) from spatial_relation_values"""
    order_qt_spatial_nonpoint_constant """select id,
        ST_Intersects(lhs, ST_GeogFromWKB('${overlap}')), ST_Intersects(ST_GeogFromWKB('${overlap}'), lhs),
        ST_Disjoint(lhs, ST_GeogFromWKB('${overlap}')), ST_Disjoint(ST_GeogFromWKB('${overlap}'), lhs)
        from spatial_relation_values"""
    // Normalized calculation rings choose the smaller region; raw WKB winding is retained.
    String smallRing = "0103000000010000000500000000000000000024c000000000000024c0000000000000244000000000000024c00000000000002440000000000000244000000000000024c0000000000000244000000000000024c000000000000024c0"
    String reverseRing = "0103000000010000000500000000000000000024c000000000000024c000000000000024c0000000000000244000000000000024400000000000002440000000000000244000000000000024c000000000000024c000000000000024c0"
    sql """insert into spatial_relation_values values
        (13, ST_GeogFromWKB('${smallRing}'), ST_GeogFromWKB('${reverseRing}'))"""
    qt_spatial_polygon_winding """select ST_Contains(lhs,
        ST_GeogFromWKB('010100000000000000000000000000000000000000')),
        ST_Contains(rhs, ST_GeogFromWKB('010100000000000000000000000000000000000000')),
        ST_Contains(lhs, ST_GeogFromWKB('010100000000000000008066400000000000000000')),
        ST_Contains(rhs, ST_GeogFromWKB('010100000000000000008066400000000000000000')),
        hex(ST_AsBinary(lhs)) = upper('${smallRing}'), hex(ST_AsBinary(rhs)) = upper('${reverseRing}')
        from spatial_relation_values where id = 13"""
    order_qt_spatial_values """
        select id, ST_AsText(geom), ST_X(geom), ST_Y(geom), ST_GeometryType(geom),
               hex(ST_AsBinary(geom)), ST_AsText(geog), ST_Distance(geog, geog)
        from geometry_values
    """
    order_qt_spatial_literal_null_arguments """
        select id, ST_Distance(geog, NULL), ST_Distance(NULL, geog),
               ST_Within(geog, NULL), ST_Azimuth(NULL, geog),
               ST_Angle(NULL, geog, geog), ST_Angle(geog, NULL, geog), ST_Angle(geog, geog, NULL)
        from geometry_values
    """
    order_qt_spatial_additional_functions """
        select id, ST_Azimuth(geog, ST_GeogFromWKB('010100000000000000000008400000000000001040')),
               ST_Within(geog, geog),
               ST_Angle(geog, ST_GeogFromWKB('010100000000000000000008400000000000001040'),
                   ST_GeogFromWKB('010100000000000000000014400000000000001840'))
        from geometry_values
    """
    test {
        sql """insert into geometry_values values (5, 'not WKB', NULL)"""
        exception "Spatial connector write cannot convert"
    }
    test {
        sql """insert into geometry_values values
            (5, NULL, ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'))"""
        exception "Spatial connector write type mismatch"
    }
    sql """insert into geometry_values select id + 10, geom, geog
        from geometry_values where id = 1"""
    order_qt_spatial_values_ordinary_grouping """
        select id, ST_AsText(any_value(geom)), ST_AsText(any_value(geog)), count(*)
        from geometry_values group by id
    """
    // Bare Iceberg spatial slots must not generate unsupported IN/Bloom runtime predicates.
    [geom: "ST_GeometryFromWKBTyped", geog: "ST_GeogFromWKB"].each { column, constructor ->
        for (int filterType : [1, 2, 8]) {
            String joinSql = """select /*+ SET_VAR(enable_runtime_filter_prune=false,
                runtime_filter_type=${filterType}) */ count(*) from geometry_values l
                join [broadcast] geometry_values r on l.${column} = r.${column}"""
            "qt_spatial_scan_join_${column}_${filterType}"(joinSql)
            explain {
                sql joinSql
                notContains "RF"
            }
        }
        for (String operator : ["in", "not in"]) {
            test {
                sql """select id from geometry_values where ${column} ${operator} (
                    ${constructor}('0101000000000000000000F03F0000000000000040'),
                    ${constructor}('010100000000000000000008400000000000001040'))"""
                exception "in predicate does not support spatial arguments"
            }
        }
    }
    order_qt_referenced_crs_source """select id, ST_AsText(geom), ST_AsText(geog)
        from referenced_crs_values"""
    [geom: "referenced_geometry_copy", geog: "referenced_geography_copy"].each { column, target ->
        for (String table : [target, target + "_partitioned"]) {
            def before = listDataObjects(table)
            for (int attempt = 0; attempt < 2; attempt++) {
                test {
                    // Two source partition keys exercise dynamic writer creation, not only the
                    // unpartitioned writer that the sink registers before opening.
                    sql """insert into ${table} select s.id + cast(n.number as int), s.${column}
                        from referenced_crs_values s cross join numbers('number' = '2') n"""
                    exception "Referenced spatial CRS"
                }
                assertEquals(before, listDataObjects(table),
                        "Rejected ${column} write left physical objects in ${table}, attempt ${attempt}")
            }
            String tag = table == target ? column : table
            "qt_referenced_crs_no_commit_${tag}"("""select
                (select count(*) from ${table}), (select count(*) from `${table}\$files`)""")
        }
    }
    minioClient.shutdown()
    sql """drop table if exists spatial_partitioned_copy"""
    sql """create table spatial_partitioned_copy (id int, geom geometry, geog geography)
        partition by list(id) () properties ('format-version' = '3')"""
    sql """insert into spatial_partitioned_copy select id, geom, geog from geometry_values"""
    order_qt_spatial_partitioned_copy """select id, ST_AsText(geom), ST_AsText(geog)
        from spatial_partitioned_copy"""

    // FULL collection should still analyze ordinary columns, while excluding every spatial
    // metadata identity. Drop prior stats so an old cache cannot make this check pass.
    for (String table : ["geometry_values", "projected_values", "referenced_crs_values"]) {
        sql """drop stats ${table}"""
        sql """analyze table ${table} with full with sync"""
        quickRunTest("spatial_analyze_${table}", "show column stats ${table}", true) { row -> row.take(5) }
    }
    for (String column : ["geom", "geog"]) {
        test {
            sql """select ndv(${column}) from geometry_values"""
            exception "does not support spatial"
        }
    }
    for (String expression : ["case when id = 1 then geom else NULL end",
            "if(id = 1, geom, NULL)", "coalesce(geom, NULL)"]) {
        test {
            sql """insert into geometry_values select id, ${expression}, NULL from projected_values"""
            exception "Spatial connector write CRS mismatch"
        }
    }
    test {
        sql """explain insert into geometry_values
            select id, cast(geom as geometry), NULL from projected_values"""
        exception "cannot cast GEOMETRY"
    }
    for (def branch : [
            ["case when id = 1 then ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040') else geom end",
             "Cannot find common type for case when"],
            ["if(id = 1, ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'), geom)",
             "Can not find the compatibility function signature"],
            ["coalesce(ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'), geom)",
             "Can not find the compatibility function signature"]]) {
        test {
            sql """explain insert into geometry_values
                select id, ${branch[0]}, NULL from projected_values"""
            exception branch[1]
        }
    }
    test {
        sql """insert into geometry_values select id, geom, NULL from projected_values"""
        exception "Spatial connector write CRS mismatch"
    }
    test {
        sql """insert into geometry_values select id, NULL, geog from projected_values"""
        exception "Spatial connector write algorithm mismatch"
    }
    order_qt_projected_values """
        select id, ST_AsText(geom), ST_X(geom), ST_Y(geom), ST_GeometryType(geom),
               hex(ST_AsBinary(geom)), ST_AsText(geog)
        from projected_values
    """
    test {
        sql """select ST_Distance(geog, geog) from projected_values"""
        exception "requires GEOGRAPHY(OGC:CRS84, spherical)"
    }
    for (String expression : ["ST_Azimuth(geog, geog)", "ST_Within(geog, geog)",
            "ST_Angle(geog, geog, geog)"]) {
        test {
            sql """select ${expression} from projected_values"""
            exception "requires GEOGRAPHY(OGC:CRS84, spherical)"
        }
    }
    sql """drop table if exists spatial_ddl_values"""
    sql """create table spatial_ddl_values (id int, geom geometry) engine=iceberg
        properties ('format-version' = '3')"""
    sql """alter table spatial_ddl_values add column geog geography"""
    sql """insert into spatial_ddl_values select id, geom, geog from geometry_values"""
    order_qt_spatial_ddl_values """
        select id, ST_AsText(geom), ST_AsText(geog) from spatial_ddl_values
    """
    sql """drop table if exists spatial_ddl_v2"""
    sql """create table spatial_ddl_v2 (id int) engine=iceberg
        properties ('format-version' = '2')"""
    test {
        sql """alter table spatial_ddl_v2 add column geom geometry"""
        exception "require format-version >= 3"
    }
    sql """drop table if exists spatial_ddl_orc"""
    sql """create table spatial_ddl_orc (id int) engine=iceberg
        properties ('format-version' = '3', 'write-format' = 'orc', 'write.format.default' = 'orc')"""
    test {
        sql """alter table spatial_ddl_orc add column geom geometry"""
        exception "require Parquet"
    }
    sql """drop table if exists spatial_create_rejected"""
    test {
        sql """create table spatial_create_rejected (geom geometry) engine=iceberg
            properties ('format-version' = '2')"""
        exception "require format-version >= 3"
    }
    test {
        sql """create table spatial_create_rejected (geom geometry) engine=iceberg
            properties ('format-version' = '3', 'write.format.default' = 'orc')"""
        exception "require Parquet"
    }
    test {
        sql """create table spatial_create_rejected (geom array<geometry>) engine=iceberg
            properties ('format-version' = '2')"""
        exception "require format-version >= 3"
    }
    // Spatial validation must preserve ordinary connector column-count diagnostics.
    test {
        sql """explain insert into spatial_ddl_v2 select 1, 2"""
        exception "Expected 1 columns but got 2"
    }
    // Exercise actual SQL analysis before changelog coercion, as well as nullable DML writeback.
    sql """drop table if exists spatial_dml_values"""
    sql """create table spatial_dml_values (id int, geom geometry, geog geography)
        engine=iceberg properties ('format-version' = '3',
            'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"""
    sql """insert into spatial_dml_values select id, geom, geog from geometry_values"""
    // UPDATE coercion rejects incompatible carriers before the spatial identity checks.
    test {
        sql """explain update spatial_dml_values set geog =
            ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040') where id = 1"""
        exception "can not cast from origin type GEOMETRY('OGC:CRS84') to target type=GEOGRAPHY"
    }
    test {
        sql """explain update spatial_dml_values set geom = 'not WKB' where id = 1"""
        exception "can not cast from origin type VARCHAR(7) to target type=GEOMETRY"
    }
    test {
        sql """explain merge into spatial_dml_values t using projected_values s on t.id = s.id
            when matched then update set geom = s.geom"""
        exception "Spatial connector write CRS mismatch"
    }
    test {
        sql """explain merge into spatial_dml_values t using projected_values s on t.id = s.id
            when matched then update set geog = s.geog"""
        exception "Spatial connector write algorithm mismatch"
    }
    test {
        sql """explain merge into spatial_dml_values t using geometry_values s on t.id = s.id
            when matched then update set geog = s.geom"""
        exception "Spatial connector write type mismatch"
    }
    test {
        sql """explain merge into spatial_dml_values t
            using (select id, geom from projected_values) s on t.id = s.id
            when matched then update set geom = s.geom"""
        exception "Spatial connector write CRS mismatch"
    }
    test {
        sql """explain with spatial_source as (select id, geom from projected_values)
            merge into spatial_dml_values t using spatial_source s on t.id = s.id
            when matched then update set geom = s.geom"""
        exception "Spatial connector write CRS mismatch"
    }
    sql """with spatial_source as (select id, geom from geometry_values)
        merge into spatial_dml_values t using spatial_source s on t.id = s.id
        when matched and s.id = 2 then update set geom = s.geom"""
    sql """update spatial_dml_values set geog = NULL where id = 1"""
    sql """merge into spatial_dml_values t using geometry_values s on t.id = s.id
        when matched and s.id = 2 then update set geom = s.geom, geog = s.geog
        when not matched then insert (id, geom, geog) values (s.id, s.geom, s.geog)"""
    order_qt_spatial_dml_values """
        select id, ST_AsText(geom), hex(ST_AsBinary(geom)), ST_AsText(geog)
        from spatial_dml_values
    """
    // A remote schema created by another engine must obey the same write restrictions.
    // Partial aggregate text states cannot preserve delimiter-bearing nested WKB.
    // Reject before execution, including the two-phase plan and both spatial kinds.
    for (String constructor : ["ST_GeometryFromWKBTyped", "ST_GeogFromWKB"]) {
        for (String limit : ["", ", 2"]) {
            test {
                sql """select /*+ SET_VAR(agg_phase=2) */ collect_list(array(
                    ${constructor}('01010000000000000000002c400000000000000040'))${limit})
                    from geometry_values"""
                exception "collect_list does not support nested spatial arguments"
            }
        }
    }
    test {
        sql """select /*+ SET_VAR(agg_phase=2) */ collect_list(shapes) from nested_spatial_values"""
        exception "collect_list does not support nested spatial arguments"
    }
    // ARRAY_AGG has a separate factory; its nonnullable branch shares the unsafe text state.
    for (String constructor : ["ST_GeometryFromWKBTyped", "ST_GeogFromWKB"]) {
        String point = "${constructor}('01010000000000000000002c400000000000000040')"
        for (String argument : ["array(${point})", "if(id = 1, array(${point}), NULL)"]) {
            for (String expression : ["array_agg(${argument})", "array_agg(distinct ${argument})",
                    "array_agg_state(${argument})", "array_agg_combine(${argument})",
                    "array_agg_merge(array_agg_state(${argument}))"]) {
                test {
                    sql """select /*+ SET_VAR(agg_phase=2, enable_agg_state=true) */
                        ${expression} from geometry_values"""
                    exception "array_agg does not support nested spatial arguments"
                }
            }
        }
    }
    test {
        sql """select /*+ SET_VAR(agg_phase=2) */ array_agg(shapes) from nested_spatial_values"""
        exception "array_agg does not support nested spatial arguments"
    }
    qt_scalar_spatial_array_agg """select /*+ SET_VAR(agg_phase=2) */
        array_sort(array_map(shape -> hex(ST_AsBinary(shape)),
            array_agg(ST_GeometryFromWKBTyped('01010000000000000000002c400000000000000040')))),
        array_sort(array_map(shape -> hex(ST_AsBinary(shape)),
            array_agg(ST_GeogFromWKB('01010000000000000000002c400000000000000040'))))
        from geometry_values where id = 1"""
    order_qt_nested_spatial_values """
        select size(shapes), ST_AsText(shapes[1]), hex(ST_AsBinary(shapes[1]))
        from nested_spatial_values
    """
    sql """insert into nested_spatial_values select * from nested_spatial_values"""
    sql """merge into nested_spatial_values t using (select 1 id) s on s.id = 1
        when matched then update set shapes = t.shapes"""
    order_qt_nested_array_writeback """select size(shapes), hex(ST_AsBinary(shapes[1]))
        from nested_spatial_values"""
    sql """merge into nested_spatial_values t using (select 1 id) s on s.id = 1
        when matched then delete"""
    qt_nested_spatial_delete_only_merge """select count(*) from nested_spatial_values"""
    // Actual external nested columns, not constructor-only values, exercise reader and writer recursion.
    sql """drop table if exists spatial_nested_ddl"""
    sql """create table spatial_nested_ddl (id int, payload struct<label:string>)
        engine=iceberg properties ('format-version'='3')"""
    sql """alter table spatial_nested_ddl add column shapes array<geometry>"""
    sql """alter table spatial_nested_ddl add column payload.shapes array<geography>"""
    sql """alter table spatial_nested_ddl modify column shapes array<geometry>"""
    sql """alter table spatial_nested_ddl modify column payload.shapes array<geography>"""
    sql """insert into spatial_nested_ddl
        select id, named_struct('label', 'row', 'shapes', array(geog)), array(geom) from geometry_values"""
    order_qt_static_nested_ddl """select id, hex(ST_AsBinary(shapes[1])),
        hex(ST_AsBinary(struct_element(payload, 'shapes')[1])) from spatial_nested_ddl"""
    test {
        sql """alter table spatial_ddl_v2 add column shapes array<geometry>"""
        exception "Iceberg spatial columns require format-version >= 3"
    }
    test {
        sql """alter table spatial_ddl_orc add column payload struct<shape:geography>"""
        exception "Iceberg spatial columns require Parquet"
    }
    order_qt_static_nested_source """select id, size(g_array),
        array_map(x -> hex(ST_AsBinary(x)), g_array),
        array_map(x -> hex(ST_AsBinary(x)), gg_array),
        hex(ST_AsBinary(g_map['shape'])), hex(ST_AsBinary(gg_map['shape'])),
        hex(ST_AsBinary(struct_element(g_struct, 'shape'))),
        hex(ST_AsBinary(struct_element(gg_struct, 'shape'))),
        array_map(x -> hex(ST_AsBinary(struct_element(x, 'shape'))), g_deep['shapes']),
        array_map(x -> hex(ST_AsBinary(struct_element(x, 'shape'))), gg_deep['shapes'])
        from spatial_nested_source"""
    sql """drop table if exists spatial_nested_copy"""
    sql """create table spatial_nested_copy (
        id int, g_array array<geometry>, g_map map<string, geometry>,
        g_struct struct<shape:geometry,label:string>, g_deep map<string,array<struct<shape:geometry>>>,
        gg_array array<geography>, gg_map map<string, geography>,
        gg_struct struct<shape:geography,label:string>, gg_deep map<string,array<struct<shape:geography>>>)
        properties ('format-version'='3')"""
    sql """insert into spatial_nested_copy select * from spatial_nested_source"""
    sql """refresh table spatial_nested_copy"""
    order_qt_static_nested_copy """select id, size(g_array),
        array_map(x -> hex(ST_AsBinary(x)), g_array),
        array_map(x -> hex(ST_AsBinary(x)), gg_array),
        hex(ST_AsBinary(g_map['shape'])), hex(ST_AsBinary(gg_map['shape'])),
        hex(ST_AsBinary(struct_element(g_struct, 'shape'))),
        hex(ST_AsBinary(struct_element(gg_struct, 'shape'))),
        array_map(x -> hex(ST_AsBinary(struct_element(x, 'shape'))), g_deep['shapes']),
        array_map(x -> hex(ST_AsBinary(struct_element(x, 'shape'))), gg_deep['shapes'])
        from spatial_nested_copy"""
    sql """drop table if exists spatial_nested_ctas"""
    sql """create table spatial_nested_ctas properties ('format-version'='3') as
        select * from spatial_nested_source"""
    order_qt_static_nested_ctas """select id, hex(ST_AsBinary(g_map['shape'])),
        hex(ST_AsBinary(struct_element(gg_struct, 'shape'))) from spatial_nested_ctas"""
    // Normalization folds these typed NULLs before target identity checks.
    sql """insert into spatial_nested_copy (id, g_array, gg_map, g_struct) values
        (7, array(CAST(NULL AS GEOMETRY('EPSG:3857'))), map('shape', CAST(NULL AS INT)),
            named_struct('shape', CAST(NULL AS INT), 'label', 'null'))"""
    qt_static_nested_typed_nulls """select id, size(g_array), g_array[1] is null,
        gg_map['shape'] is null, struct_element(g_struct, 'shape') is null
        from spatial_nested_copy where id = 7"""
    for (String expression : ["array(geom)",
            "if(id = 1, array(geom), cast(NULL as array<geometry('EPSG:3857')>))"]) {
        test {
            sql """insert into spatial_nested_copy (id, g_array) select id + 10,
                ${expression} from projected_values"""
            exception "CRS mismatch"
        }
    }
    test {
        sql """insert into spatial_nested_copy (id, g_array)
            select id + 10, cast(array(geom) as array<geometry>) from projected_values"""
        exception "can not cast"
    }
    test {
        sql """insert into spatial_nested_copy (id, g_map)
            select id + 20, map('shape', geog) from geometry_values"""
        exception "type mismatch"
    }
    test {
        sql """insert into spatial_nested_copy (id, gg_struct) select id + 30,
            named_struct('shape', geog, 'label', 'wrong') from projected_values"""
        exception "algorithm mismatch"
    }
    // Partition keys require semantic equality; ordinary keys may still carry spatial window values.
    for (String column : ["geom", "geog"]) {
        for (String key : [column, "array(${column})", "map(id, ${column})",
                "named_struct('shape', ${column})"]) {
            test {
                sql """select id, count(*) over (partition by ${key}) from geometry_values"""
                exception "PARTITION BY does not support spatial keys"
            }
        }
        for (String function : ["count_at_min", "count_at_max"]) {
            for (String expression : ["${function}(${column})", "${function}(array(${column}))",
                    "${function}(${column}) over ()", "${function}_state(${column})"]) {
                test {
                    sql """select /*+ SET_VAR(enable_agg_state=true) */ ${expression} from geometry_values"""
                    exception "does not support spatial arguments"
                }
            }
        }
        for (String value : [column, "array(${column})", "map('shape', ${column})",
                "named_struct('shape', ${column})"]) {
            test {
                sql """select cast(${value} as variant) from geometry_values"""
                exception (value == column ? "cannot cast" : "can not cast")
            }
        }
    }
    order_qt_static_spatial_window_values """select id,
        hex(ST_AsBinary(first_value(geom) over (partition by id order by id))),
        hex(ST_AsBinary(last_value(geog) over (partition by id order by id)))
        from geometry_values"""
    qt_static_ordinary_extremes_variant """select count_at_min(id), count_at_max(id),
        count(cast(array(id) as variant)) from geometry_values"""
    sql """drop table if exists spatial_variant_neighbors"""
    sql """create table spatial_variant_neighbors (id int, geom geometry, geog geography, payload variant)
        engine=iceberg properties ('format-version'='3')"""
    sql """insert into spatial_variant_neighbors
        select id, geom, geog, cast(array(id) as variant) from geometry_values"""
    sql """refresh table spatial_variant_neighbors"""
    order_qt_static_variant_neighbors """select id, hex(ST_AsBinary(geom)), hex(ST_AsBinary(geog)),
        cast(payload as string) from spatial_variant_neighbors"""
    sql """drop table if exists spatial_wkb_values"""
    sql """create table spatial_wkb_values (id int, geom geometry, geog geography)
        engine=iceberg properties ('format-version' = '3')"""
    sql """insert into spatial_wkb_values values
        (1, ST_GeometryFromWKBTyped('0104000000020000000101000000000000000000F03F0000000000000040000000000140080000000000004010000000000000'),
            ST_GeogFromWKB('0104000000020000000101000000000000000000F03F0000000000000040000000000140080000000000004010000000000000')),
        (2, ST_GeometryFromWKBTyped('010500000001000000010200000000000000'),
            ST_GeogFromWKB('010500000001000000010200000000000000')),
        (3, ST_GeometryFromWKBTyped('010600000001000000010300000000000000'),
            ST_GeogFromWKB('010600000001000000010300000000000000')),
        (4, ST_GeometryFromWKBTyped('010700000001000000010700000000000000'),
            ST_GeogFromWKB('010700000001000000010700000000000000')),
        (5, ST_GeometryFromWKBTyped('0101000000000000000000F87F000000000000F87F'),
            ST_GeogFromWKB('0101000000000000000000F87F000000000000F87F')),
        (6, ST_GeometryFromWKBTyped('010200000000000000'), ST_GeogFromWKB('010200000000000000')),
        (7, ST_GeometryFromWKBTyped('010300000000000000'), ST_GeogFromWKB('010300000000000000')),
        (8, NULL, NULL),
        (9, ST_GeometryFromWKBTyped('0101000000000000000000F03F000000000000F87F'),
            ST_GeogFromWKB('0101000000000000000000F03F000000000000F87F')),
        (10, ST_GeometryFromWKBTyped('010200000002000000000000000000F03F000000000000004000000000000008400000000000001040'),
            ST_GeogFromWKB('010200000002000000000000000000F03F000000000000004000000000000008400000000000001040')),
        (11, ST_GeometryFromWKBTyped('0103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000'),
            ST_GeogFromWKB('0103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000')),
        (12, ST_GeometryFromWKBTyped('010500000001000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040'),
            ST_GeogFromWKB('010500000001000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040')),
        (13, ST_GeometryFromWKBTyped('0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000'),
            ST_GeogFromWKB('0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000')),
        (14, ST_GeometryFromWKBTyped('0107000000030000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000'),
            ST_GeogFromWKB('0107000000030000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F0000000000000040000000000000084000000000000010400103000000010000000400000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000004000000000000000000000000000000000'))"""
    order_qt_spatial_wkb_values """
        select id, geom is null, geog is null, ST_AsText(geom), ST_AsText(geog),
               ST_GeometryType(geom), ST_X(geom), ST_Y(geog), hex(ST_AsBinary(geom))
        from spatial_wkb_values
    """
    // Exercise FE binding on scanned Geography columns, not only direct BE function calls.
    order_qt_spatial_area_columns """select id,
        round(ST_Area_Square_Meters(geog), 3), round(ST_Area_Square_Km(geog), 6),
        abs(ST_Area_Square_Meters(geog) / 1000000 - ST_Area_Square_Km(geog)) < 0.000001
        from spatial_wkb_values where id in (8, 11)"""
    qt_spatial_area_constants """select
        ST_Area_Square_Meters(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')),
        ST_Area_Square_Km(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')),
        ST_Area_Square_Meters(cast(NULL as geography)),
        ST_Area_Square_Km(cast(NULL as geography))"""
    qt_spatial_area_legacy """select
        round(ST_Area_Square_Meters(ST_Polygon('POLYGON ((0 0, 2 0, 0 2, 0 0))')), 3),
        round(ST_Area_Square_Km(ST_Polygon('POLYGON ((0 0, 2 0, 0 2, 0 0))')), 6),
        ST_Area_Square_Meters(NULL), ST_Area_Square_Km(NULL)"""
    for (String function : ["ST_Area_Square_Meters", "ST_Area_Square_Km"]) {
        for (String source : ["spatial_wkb_values", "projected_values"]) {
            test {
                sql """select ${function}(geom) from ${source} where id = 1"""
                exception "requires GEOGRAPHY(OGC:CRS84, spherical)"
            }
        }
        for (String source : ["projected_values", "referenced_crs_values"]) {
            test {
                sql """select ${function}(geog) from ${source} where id = 1"""
                exception "requires GEOGRAPHY(OGC:CRS84, spherical)"
            }
        }
    }
    test {
        sql """select ST_Length(geog) from spatial_wkb_values where id = 1"""
        exception "does not support this Geography WKB shape"
    }
    sql """drop table if exists projected_copy"""
    sql """create table projected_copy (
        id int, geom geometry('EPSG:3857'), geog geography('OGC:CRS84', 'vincenty')
        ) engine=iceberg properties ('format-version' = '3')"""
    sql """insert into projected_copy select id, geom, geog from projected_values"""
    // A generic file TVF must not infer spatial identity outside the Iceberg catalog contract.
    def spatialFile = (sql """select file_path from `projected_copy\$files` where content = 0""")[0][0]
    test {
        sql """select * from S3(
            'uri' = '${spatialFile}', 'format' = 'parquet',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.region' = 'us-east-1')"""
        exception "Spatial Parquet columns are only supported through an Iceberg catalog"
    }
    // An unannotated BYTE_ARRAY remains binary when read without Iceberg schema metadata.
    def rawFile = (sql """select file_path from `projected_values\$files` where content = 0""")[0][0]
    order_qt_raw_file_bytes """select id, from_binary(geom), from_binary(geog) from S3(
        'uri' = '${rawFile}', 'format' = 'parquet',
        's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
        's3.access_key' = 'admin', 's3.secret_key' = 'password',
        's3.region' = 'us-east-1')"""
    // Materialize payloads in both readers: COUNT pushdown alone can bypass WKB conversion.
    // geometry_values has spatial Parquet annotations; projected_values imports unannotated WKB.
    for (boolean scanV2 : [false, true]) {
        "qt_annotated_spatial_payload_${scanV2}"("""select /*+ SET_VAR(
            enable_file_scanner_v2=${scanV2}) */ id,
            geom is null, ST_AsText(geom), hex(ST_AsBinary(geom)),
            geog is null, ST_AsText(geog), hex(ST_AsBinary(geog))
            from geometry_values order by id""")
        "qt_unannotated_spatial_payload_${scanV2}"("""select /*+ SET_VAR(
            enable_file_scanner_v2=${scanV2}) */ id,
            geom is null, ST_AsText(geom), hex(ST_AsBinary(geom)),
            geog is null, ST_AsText(geog), hex(ST_AsBinary(geog))
            from projected_values order by id""")
    }
    sql """drop table if exists spatial_zm_values"""
    sql """create table spatial_zm_values (id int, geom geometry, geog geography)
        engine=iceberg properties ('format-version' = '3')"""
    sql """insert into spatial_zm_values values
        (1, ST_GeometryFromWKBTyped('01E9030000000000000000F03F00000000000000400000000000000840'),
            ST_GeogFromWKB('01E9030000000000000000F03F00000000000000400000000000000840')),
        (2, ST_GeometryFromWKBTyped('01D1070000000000000000F03F00000000000000400000000000000840'),
            ST_GeogFromWKB('01D1070000000000000000F03F00000000000000400000000000000840')),
        (3, ST_GeometryFromWKBTyped('01B90B0000000000000000F03F000000000000004000000000000008400000000000001040'),
            ST_GeogFromWKB('01B90B0000000000000000F03F000000000000004000000000000008400000000000001040')),
        (4, NULL, NULL),
        (5, ST_GeometryFromWKBTyped('01EB03000000000000'), ST_GeogFromWKB('01EB03000000000000'))"""
    sql """drop table if exists spatial_zm_copy"""
    sql """create table spatial_zm_copy (id int, geom geometry, geog geography)
        engine=iceberg properties ('format-version' = '3')"""
    sql """insert into spatial_zm_copy select id, geom, geog from spatial_zm_values"""
    for (boolean scanV2 : [false, true]) {
        "qt_spatial_zm_payload_${scanV2}"("""select /*+ SET_VAR(enable_file_scanner_v2=${scanV2}) */
            id, ST_AsText(geom), ST_AsText(geog), ST_X(geom), ST_Y(geog),
            hex(ST_AsBinary(geom)), hex(ST_AsBinary(geog)), ST_GeometryType(geom)
            from spatial_zm_copy order by id""")
    }
    for (String query : ["ST_Length(geog)", "ST_Distance(geog, geog)", "ST_Azimuth(geog, geog)",
            "ST_Area_Square_Meters(geog)", "ST_Area_Square_Km(geog)"]) {
        test {
            sql """select ${query} from spatial_zm_values where id = 1"""
            exception "does not support this Geography WKB shape"
        }
    }
    // Recreate the catalog so the second read resolves schema and files from external metadata.
    createCatalog()
    order_qt_spatial_zm_reread """select id, ST_AsText(geom), ST_AsText(geog),
        hex(ST_AsBinary(geom)), hex(ST_AsBinary(geog)) from spatial_zm_copy"""
    order_qt_spatial_dml_reread """
        select id, ST_AsText(geom), hex(ST_AsBinary(geom)), ST_AsText(geog)
        from spatial_dml_values
    """
    order_qt_spatial_wkb_reread """
        select id, geom is null, geog is null, ST_AsText(geom), ST_AsText(geog),
               ST_GeometryType(geom), ST_X(geom), ST_Y(geog), hex(ST_AsBinary(geom))
        from spatial_wkb_values
    """
    qt_projected_copy_schema """desc projected_copy"""
    order_qt_projected_copy_reread """
        select id, ST_AsText(geom), ST_X(geom), ST_Y(geom), hex(ST_AsBinary(geom)), ST_AsText(geog)
        from projected_copy
    """
    order_qt_spatial_reread """
        select id, ST_AsText(geom), hex(ST_AsBinary(geom)), ST_AsText(geog)
        from geometry_values
    """
    for (int phase : [3, 4]) {
        "qt_ordinary_distinct_count_map_${phase}"("""select /*+ SET_VAR(agg_phase=${phase}) */
            element_at(count_map(distinct map(1, number % 2)), 1) from numbers('number'='4')""")
    }
    for (int phase : [1, 2]) {
        "qt_ordinary_multi_distinct_arrays_${phase}"("""select /*+ SET_VAR(agg_phase=${phase}) */
            multi_distinct_count(number % 2), multi_distinct_count(array(number % 2)),
            multi_distinct_count(array(array(number % 2))),
            multi_distinct_count(array(CAST(NULL AS INT))),
            multi_distinct_count(CAST(NULL AS ARRAY<INT>)) from numbers('number'='4')""")
    }
    for (String expression : ["shapes", "array(shapes)"]) {
        test {
            sql """select multi_distinct_count(${expression}) from nested_spatial_values"""
            exception "multi_distinct_count does not support spatial arguments"
        }
    }
    // Append after existing checks: identical points with different endian encodings and a NULL.
    sql """insert into geometry_values values
        (101, ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'),
            ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')),
        (102, ST_GeometryFromWKBTyped('00000000013FF00000000000004000000000000000'),
            ST_GeogFromWKB('00000000013FF00000000000004000000000000000')),
        (103, NULL, NULL)"""
    for (String column : ["geom", "geog"]) {
        for (String operation : ["union", "union distinct", "intersect", "except"]) {
            test {
                sql """select ${column} from geometry_values where id = 101 ${operation}
                    select ${column} from geometry_values where id = 102"""
                exception "GROUP BY and DISTINCT do not support spatial keys"
            }
        }
        // The explicit aggregate has no DISTINCT flag; reject spatial leaves recursively.
        for (int phase : [1, 2]) {
            for (String expression : ["array(${column})", "array(array(${column}))",
                    "CAST(NULL AS ARRAY<${column == 'geom' ? 'GEOMETRY' : 'GEOGRAPHY'}>)"]) {
                test {
                    sql """select /*+ SET_VAR(agg_phase=${phase}) */ multi_distinct_count(${expression})
                        from geometry_values where id in (101, 102, 103)"""
                    exception "multi_distinct_count does not support spatial arguments"
                }
            }
        }
        // DISTINCT would turn the whole MAP into a later physical grouping key.
        // Reject raw WKB comparisons for both byte orders and NULLs before lowering.
        for (int phase : [3, 4]) {
            for (String aggregate : ["count_map", "min_map", "max_map"]) {
                for (String value : [column, "array(${column})", "map(id, ${column})",
                        "named_struct('shape', ${column})"]) {
                    test {
                        sql """select /*+ SET_VAR(agg_phase=${phase}) */
                            ${aggregate}(distinct map(1, ${value})) from geometry_values
                            where id in (101, 102, 103)"""
                        exception "${aggregate} DISTINCT does not support spatial arguments"
                    }
                }
            }
            test {
                sql """select /*+ SET_VAR(agg_phase=${phase}) */
                    count_map(distinct CAST(NULL AS MAP<INT, ${column == 'geom' ? 'GEOMETRY' : 'GEOGRAPHY'}>))
                    from geometry_values where id in (101, 102, 103)"""
                exception "count_map DISTINCT does not support spatial arguments"
            }
            "qt_spatial_plain_count_map_${column}_${phase}"("""select /*+ SET_VAR(agg_phase=${phase}) */
                element_at(count_map(map(1, ${column})), 1) from geometry_values
                where id in (101, 102, 103)""")
        }
        for (String aggregate : ["array_agg", "collect_list"]) {
            for (int phase : [1, 2]) {
                test {
                    sql """select /*+ SET_VAR(agg_phase=${phase}) */ ${aggregate}(distinct ${column})
                        from geometry_values where id in (101, 102, 103)"""
                    exception "${aggregate} DISTINCT does not support spatial arguments"
                }
            }
        }
        test {
            sql """select collect_list(distinct ${column}, 2)
                from geometry_values where id in (101, 102, 103)"""
            exception "collect_list DISTINCT does not support spatial arguments"
        }
        "order_qt_spatial_union_all_${column}"("""select ST_AsText(shape), hex(ST_AsBinary(shape))
            from (select ${column} shape from geometry_values where id = 101 union all
                select ${column} shape from geometry_values where id = 102) s""")
        "qt_spatial_plain_lists_${column}"("""select size(array_agg(${column})),
            size(collect_list(${column})), size(collect_list(${column}, 2))
            from geometry_values where id in (101, 102, 103)""")
    }
}
