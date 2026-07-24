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

suite("test_iceberg_partition_evolution_equality_delete",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_partition_evolution_equality_delete"
    String dbName = "iceberg_partition_evolution_equality_delete_db"
    String tableName = "equality_delete_evolved"

    def stringRows = { String query ->
        sql(query).collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def latestSnapshotId = {
        // The Java helper commits through the REST catalog directly, so invalidate Spark's
        // cached table metadata before recording the snapshot used by time-travel assertions.
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        return spark_iceberg("""
            select snapshot_id
            from demo.${dbName}.${tableName}.snapshots
            order by committed_at desc
            limit 1
        """)[0][0].toString()
    }
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
                // A shared external environment contains multiple services; only Spark has the
                // Iceberg jars needed to construct a real equality-delete file.
            }
        }
        assertEquals(1, matches.size(), "Expected exactly one usable Spark Iceberg container")
        sparkContainer = matches[0]
    }
    def runInSparkContainer = { String command ->
        executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '${command}'", 300)
    }

    String javaSource = '''
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class AppendEvolutionEqualityDelete {
    public static void main(String[] args) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "rest");
        props.put("uri", "http://rest:8181");
        props.put("warehouse", "s3://warehouse/wh/");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", "http://minio:9000");
        props.put("s3.path-style-access", "true");
        props.put("s3.region", "us-east-1");
        Catalog catalog = CatalogUtil.buildIcebergCatalog("demo", props, null);
        Table table = catalog.loadTable(TableIdentifier.of(args[0], args[1]));
        if (!table.spec().isUnpartitioned()) {
            throw new IllegalStateException("Equality-delete checkpoints must use an unpartitioned spec");
        }
        String fieldName = args[2];
        Schema equalitySchema = table.schema().select(fieldName);
        int fieldId = table.schema().findField(fieldName).fieldId();
        OutputFile output = table.io().newOutputFile(
                table.location() + "/data/evolution-equality-delete-" + args[3]
                        + "-" + System.currentTimeMillis() + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(output)
                .forTable(table)
                .rowSchema(equalitySchema)
                .withSpec(PartitionSpec.unpartitioned())
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(fieldId)
                .overwrite()
                .buildEqualityWriter();
        GenericRecord record = GenericRecord.create(equalitySchema);
        record.setField(fieldName, Integer.valueOf(args[3]));
        writer.write(record);
        writer.close();
        DeleteFile deleteFile = writer.toDeleteFile();
        table.newRowDelta().addDeletes(deleteFile).commit();
    }
}
'''
    String encodedJava = javaSource.getBytes("UTF-8").encodeBase64().toString()

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0'
        )
    """

    try {
        runInSparkContainer(
                "echo ${encodedJava} | base64 -d >/tmp/AppendEvolutionEqualityDelete.java && "
                        + "javac -cp \"/opt/spark/jars/*\" "
                        + "/tmp/AppendEvolutionEqualityDelete.java")
        spark_iceberg_multi """
            create database if not exists demo.${dbName};
            drop table if exists demo.${dbName}.${tableName};
            create table demo.${dbName}.${tableName} (
                id int,
                category string,
                event_time timestamp,
                payload struct<metric:int, label:string>
            ) using iceberg
            tblproperties (
                'format-version'='2',
                'write.format.default'='parquet'
            );
            insert into demo.${dbName}.${tableName} values
                (1, 'A', timestamp '2026-01-01 01:00:00',
                    named_struct('metric', 10, 'label', 'base-a')),
                (2, 'A', timestamp '2026-01-01 02:00:00',
                    named_struct('metric', 20, 'label', 'base-delete')),
                (3, 'B', timestamp '2026-01-02 01:00:00',
                    named_struct('metric', 30, 'label', 'base-b')),
                (4, 'C', timestamp '2026-01-03 01:00:00',
                    named_struct('metric', 40, 'label', 'base-c'));
        """
        String baseSnapshot = latestSnapshotId()
        sql """
            alter table `${catalogName}`.`${dbName}`.`${tableName}`
            create tag equality_base as of version ${baseSnapshot}
        """

        // Scenario PE-EQ01: an equality delete written under the original unpartitioned spec
        // must remain effective after later partition specs are added.
        runInSparkContainer(
                "java -cp \"/tmp:/opt/spark/jars/*\" AppendEvolutionEqualityDelete "
                        + "${dbName} ${tableName} id 2")
        String firstDeleteSnapshot = latestSnapshotId()

        // Scenario PE-EQ02: ADD identity/day/bucket fields and a nested child after the delete.
        spark_iceberg_multi """
            alter table demo.${dbName}.${tableName} add partition field category;
            alter table demo.${dbName}.${tableName} add partition field days(event_time);
            alter table demo.${dbName}.${tableName} add partition field bucket(8, id);
            alter table demo.${dbName}.${tableName} add column payload.extra string;
            insert into demo.${dbName}.${tableName} values
                (5, 'A', timestamp '2026-02-01 01:00:00',
                    named_struct('metric', 50, 'label', 'added-a', 'extra', 'new-child')),
                (6, 'A', timestamp '2026-02-01 02:00:00',
                    named_struct('metric', 60, 'label', 'added-delete', 'extra', 'new-child'));
        """
        String addedSnapshot = latestSnapshotId()

        // Scenario PE-EQ03: REPLACE day -> month while renaming/promoting nested fields.
        spark_iceberg_multi """
            alter table demo.${dbName}.${tableName}
                replace partition field days(event_time) with months(event_time);
            alter table demo.${dbName}.${tableName}
                rename column payload.label to renamed_label;
            alter table demo.${dbName}.${tableName}
                alter column payload.metric type bigint;
            insert into demo.${dbName}.${tableName} values
                (7, 'A', timestamp '2026-03-01 01:00:00',
                    named_struct('metric', 7000000000,
                        'renamed_label', 'replace-a', 'extra', 'renamed-child')),
                (8, 'A', timestamp '2026-03-01 02:00:00',
                    named_struct('metric', 80,
                        'renamed_label', 'replace-delete', 'extra', 'renamed-child'));
        """

        // Scenario PE-EQ04: return to an unpartitioned spec and write equality deletes after
        // evolution. This covers delete files both before and after the multi-spec interval.
        spark_iceberg_multi """
            alter table demo.${dbName}.${tableName} drop partition field category;
            alter table demo.${dbName}.${tableName} drop partition field months(event_time);
            alter table demo.${dbName}.${tableName} drop partition field bucket(8, id);
        """
        runInSparkContainer(
                "java -cp \"/tmp:/opt/spark/jars/*\" AppendEvolutionEqualityDelete "
                        + "${dbName} ${tableName} id 6")
        runInSparkContainer(
                "java -cp \"/tmp:/opt/spark/jars/*\" AppendEvolutionEqualityDelete "
                        + "${dbName} ${tableName} id 8")
        String finalSnapshot = latestSnapshotId()
        sql """
            alter table `${catalogName}`.`${dbName}`.`${tableName}`
            create tag equality_final as of version ${finalSnapshot}
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh catalog ${catalogName}"""

        // Scenario PE-EQ05: partition-source filters honor equality deletes across every spec.
        List<List<String>> expectedCurrent = [["1"], ["5"], ["7"]]
        assertEquals(expectedCurrent, stringRows("""
            select id from ${tableName} where category = 'A' order by id
        """))
        assertEquals([["7", "7000000000"]], stringRows("""
            select id, payload.metric from ${tableName}
            where event_time >= timestamp '2026-03-01 00:00:00'
              and payload.metric > 5000000000
            order by id
        """))

        // Scenario PE-EQ06: numeric snapshots and tags select the matching delete/spec state.
        assertEquals([["1"], ["2"]], stringRows("""
            select id from ${tableName}@tag(equality_base)
            where category = 'A' order by id
        """))
        assertEquals([["1"]], stringRows("""
            select id from ${tableName} for version as of ${firstDeleteSnapshot}
            where category = 'A' order by id
        """))
        assertEquals([["1"], ["5"], ["6"]], stringRows("""
            select id from ${tableName} for version as of ${addedSnapshot}
            where category = 'A' order by id
        """))
        assertEquals(expectedCurrent, stringRows("""
            select id from ${tableName}@tag(equality_final)
            where category = 'A' order by id
        """))

        // Scenario PE-EQ07: both scanner implementations apply the same equality deletes.
        sql """set enable_file_scanner_v2=true"""
        List<List<String>> scannerV2Rows = stringRows("""
            select id from ${tableName} where category = 'A' order by id
        """)
        sql """set enable_file_scanner_v2=false"""
        assertEquals(scannerV2Rows, stringRows("""
            select id from ${tableName} where category = 'A' order by id
        """))

        List<List<String>> equalityFiles = stringRows("""
            select file_format from ${tableName}\$all_files
            where content = 2 order by file_path
        """)
        assertEquals(3, equalityFiles.size(),
                "${tableName} must contain all three equality-delete files")
    } finally {
        sql """set enable_file_scanner_v2=true"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
