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
    order_qt_spatial_grouping """
        select ST_AsText(geom), ST_AsText(geog), count(*)
        from geometry_values group by geom, geog
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
            properties ('format-version' = '3')"""
        // ColumnDefinition validates supported ARRAY elements before connector DDL validation.
        exception "ARRAY unsupported sub-type: GEOMETRY"
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
    test {
        sql """explain update spatial_dml_values set geog =
            ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040') where id = 1"""
        exception "Spatial connector write type mismatch"
    }
    test {
        sql """explain update spatial_dml_values set geom = 'not WKB' where id = 1"""
        exception "Spatial connector write cannot convert"
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
    order_qt_nested_spatial_values """
        select size(shapes), ST_AsText(shapes[1]), hex(ST_AsBinary(shapes[1]))
        from nested_spatial_values
    """
    test {
        sql """insert into nested_spatial_values select * from nested_spatial_values"""
        exception "Nested spatial columns"
    }
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
    // Recreate the catalog so the second read resolves schema and files from external metadata.
    createCatalog()
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
}
