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

suite("test_iceberg_spatial_v3", "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_spatial_v3"
    String dbName = "iceberg_spatial_v3_db"
    String tableName = "spatial_values"
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
            String probe = "${dockerCommand} exec ${containerId} bash -lc "
                    + "'test -f /mnt/SUCCESS && command -v spark-sql >/dev/null'"
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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public class CreateIcebergSpatialV3Table {
    public static void main(String[] args) {
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
    }
}
'''
    String encodedJavaSource = javaSource.getBytes("UTF-8").encodeBase64().toString()
    executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc 'echo ${encodedJavaSource} "
            + "| base64 -d >/tmp/CreateIcebergSpatialV3Table.java && "
            + "javac -cp \"/opt/spark/jars/*\" /tmp/CreateIcebergSpatialV3Table.java && "
            + "java -cp \"/tmp:/opt/spark/jars/*\" CreateIcebergSpatialV3Table ${dbName} ${tableName}'")

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

    try {
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_fallback_to_original_planner = false"""
        sql """
            insert into ${tableName} values
                (1, ST_GeomFromWKB('0101000000000000000000F03F0000000000000040'),
                 ST_GeogFromWKB('0101000000000000000000F03F0000000000000040'))
        """
        def rows = sql """
            select id, ST_AsText(geom), ST_AsText(geog)
            from ${tableName}
            order by id
        """
        assertEquals(1, rows.size())
        assertEquals(1, rows[0][0].toString().toInteger())
        assertEquals("POINT (1 2)", rows[0][1].toString())
        assertEquals("POINT (1 2)", rows[0][2].toString())
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
