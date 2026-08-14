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

import java.util.regex.Matcher
import java.util.regex.Pattern
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.doris.regression.action.ProfileAction

suite("test_iceberg_variant_read",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }
    // Iceberg VARIANT carries the external computeV2 physical marker and must work with either
    // native Doris VARIANT mode selected by the cluster's FE config.
    logger.info("run Iceberg VARIANT with FE enable_variant_v2={}",
            getFeConfig("enable_variant_v2"))

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restUri = context.config.otherConfigs.get("iceberg_rest_uri")
    if (restUri == null) {
        restUri = "http://${externalEnvIp}:${restPort}"
    }
    String catalogName = "test_iceberg_variant_read"
    String dbName = "iceberg_variant_read_db"
    String fixtureKey = "doris-regression/iceberg-variant/iceberg_variant_shredded.parquet"
    File shreddedFixture = new File(context.dataPath, "iceberg_variant_shredded.parquet")
    File shreddedTableFixture = new File(context.dataPath, "iceberg_variant_shredded_table")
    String shreddedMetadataName =
            "00002-5d3f3ae6-7100-4eb0-a42e-e52ddc62d9e3.metadata.json"
    assertTrue(shreddedFixture.isFile(), "Missing shredded Variant Parquet fixture")
    assertTrue(shreddedTableFixture.isDirectory(), "Missing shredded Variant table fixture")
    def credentials = new BasicAWSCredentials("admin", "password")
    def endpoint = new EndpointConfiguration(
            "http://${externalEnvIp}:${minioPort}", "us-east-1")
    def minioClient = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(endpoint)
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build()
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
                // Only the Spark service contains the Iceberg writer dependencies.
            }
        }
        assertEquals(1, matches.size(), "Expected exactly one usable Spark Iceberg container")
        sparkContainer = matches[0]
    }
    def runInSparkContainer = { String command ->
        executeCommand("${dockerCommand} exec ${sparkContainer} bash -lc '${command}'", 300)
    }

    def latestSnapshotId = { String tableName ->
        List<List<Object>> rows = spark_iceberg """
            SELECT snapshot_id
            FROM demo.${dbName}.${tableName}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    spark_iceberg_multi """
        CREATE NAMESPACE IF NOT EXISTS demo.${dbName};
        DROP TABLE IF EXISTS demo.${dbName}.variant_values;
        CREATE TABLE demo.${dbName}.variant_values (
            id INT,
            v VARIANT
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.merge.mode'='merge-on-read'
        );
        INSERT INTO demo.${dbName}.variant_values VALUES
            (1, parse_json('{"name":"alice","n":10,"ratio":1.5,"ok":true,"arr":[1,2],"nested":{"city":"hz"}}')),
            (2, parse_json('{"name":"bob","n":20,"ratio":2.5,"ok":false,"arr":[3,4],"nested":{"city":"sh"}}')),
            (3, parse_json('{"name":"same","n":30,"ok":true}')),
            (4, parse_json('null')),
            (5, NULL),
            (6, parse_json('42')),
            (7, parse_json('"root-string"'));
        ALTER TABLE demo.${dbName}.variant_values SET TBLPROPERTIES (
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_values
        VALUES
            (8, parse_json('{"ok":true,"n":30,"name":"same"}')),
            (9, parse_json('{"name":"carol","n":40,"ratio":4.5,"ok":true,"arr":[5,6],"nested":{"city":"bj"},"new_key":"new"}')),
            (10, parse_json('{"name":"dave","n":50,"ratio":5.5,"ok":false,"arr":[7,8],"nested":{"city":"sz"}}')),
            (11, parse_json('{"name":null,"n":60,"ratio":6.5,"ok":true,"arr":[9,10],"nested":{"city":null}}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_deep_object;
        CREATE TABLE demo.${dbName}.variant_deep_object (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );
        INSERT INTO demo.${dbName}.variant_deep_object VALUES
            (1, parse_json('{"profile":{"address":{"country":{"code":"CN","rank":1}}}}')),
            (2, parse_json('{"profile":{"address":{"country":{"code":"US","rank":2}}}}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_root_arrays;
        CREATE TABLE demo.${dbName}.variant_root_arrays (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_root_arrays VALUES
            (1, parse_json('[]')),
            (2, parse_json('[null,1,{"x":2},[3,4],"tail"]')),
            (3, parse_json('[{"nested":[null,{"y":5}]}]')),
            (4, parse_json('null')),
            (5, NULL);

        DROP TABLE IF EXISTS demo.${dbName}.variant_multi_file;
        CREATE TABLE demo.${dbName}.variant_multi_file (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );
        INSERT INTO demo.${dbName}.variant_multi_file
            VALUES (1, parse_json('{"a":1,"shared":10}'));
        ALTER TABLE demo.${dbName}.variant_multi_file SET TBLPROPERTIES (
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='1'
        );
        INSERT INTO demo.${dbName}.variant_multi_file
            VALUES (2, parse_json('{"b":2,"shared":20,"z":200}'));
        INSERT INTO demo.${dbName}.variant_multi_file
            VALUES (3, parse_json('{"z":300,"shared":30,"a":3}'));
        ALTER TABLE demo.${dbName}.variant_multi_file SET TBLPROPERTIES
            ('write.parquet.shred-variants'='false');
        INSERT INTO demo.${dbName}.variant_multi_file
            VALUES (4, parse_json('{"c":4,"shared":40}'));
        ALTER TABLE demo.${dbName}.variant_multi_file SET TBLPROPERTIES
            ('write.parquet.shred-variants'='false');
        INSERT INTO demo.${dbName}.variant_multi_file
            VALUES (5, parse_json('{"shared":50,"b":5,"new_field":{"k":500}}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_type_matrix;
        CREATE TABLE demo.${dbName}.variant_type_matrix (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_type_matrix SELECT 1, to_variant_object(named_struct(
            'bool_value', true,
            'tiny_value', CAST(-128 AS TINYINT),
            'small_value', CAST(-32768 AS SMALLINT),
            'int_value', CAST(2147483647 AS INT),
            'big_value', CAST('-9223372036854775808' AS BIGINT),
            'float_value', CAST('NaN' AS FLOAT),
            'double_value', CAST('Infinity' AS DOUBLE),
            'decimal_value', CAST('-1234567890.1234' AS DECIMAL(20, 4)),
            'date_value', CAST('1970-01-02' AS DATE),
            'timestamp_value', TIMESTAMP'1970-01-01 00:00:01.234567',
            'binary_value', CAST('binary' AS BINARY),
            'null_value', CAST(NULL AS INT)
        ));

        DROP TABLE IF EXISTS demo.${dbName}.variant_multi_row_group;
        CREATE TABLE demo.${dbName}.variant_multi_row_group (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100',
            'write.parquet.row-group-size-bytes'='4096'
        );
        SET spark.sql.shuffle.partitions=1;
        INSERT INTO demo.${dbName}.variant_multi_row_group
        SELECT /*+ COALESCE(1) */ CAST(id AS INT), parse_json(concat(
            '{"n":', id, ',"padding":"', repeat('x', 256), '"}'))
        FROM range(0, 8192);

        DROP TABLE IF EXISTS demo.${dbName}.variant_doris_multi_row_group;
        CREATE TABLE demo.${dbName}.variant_doris_multi_row_group (
            id BIGINT,
            info STRUCT<label: STRING, payload: VARIANT>,
            padding STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );

        DROP TABLE IF EXISTS demo.${dbName}.variant_deletion_vector;
        CREATE TABLE demo.${dbName}.variant_deletion_vector (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100',
            'write.delete.mode'='merge-on-read',
            'read.parquet.vectorization.enabled'='false',
            'write.parquet.row-group-size-bytes'='4096'
        );
        INSERT INTO demo.${dbName}.variant_deletion_vector
        SELECT /*+ COALESCE(1) */ CAST(id AS INT), parse_json(concat('{"n":', id, ',"keep":',
            IF(id % 2 = 0, 'true', 'false'), '}'))
        FROM range(0, 4096);
        RESET spark.sql.shuffle.partitions;

        DROP TABLE IF EXISTS demo.${dbName}.variant_equality_delete;
        CREATE TABLE demo.${dbName}.variant_equality_delete (id INT, v VARIANT) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_equality_delete VALUES
            (1, parse_json('{"n":10,"label":"keep-one"}')),
            (2, parse_json('{"n":20,"label":"delete"}')),
            (3, parse_json('{"n":30,"label":"keep-three"}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_page_pruning;

        DROP TABLE IF EXISTS demo.${dbName}.variant_nested;
        CREATE TABLE demo.${dbName}.variant_nested (
            id INT,
            info STRUCT<label: STRING, payload: VARIANT>,
            events ARRAY<VARIANT>,
            attrs MAP<STRING, VARIANT>
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_nested SELECT
            1,
            named_struct('label', 'first', 'payload', parse_json('{"x":11,"deep":{"name":"inside"}}')),
            array(parse_json('{"kind":"open","score":101}'), parse_json('2')),
            map('primary', parse_json('{"enabled":true,"score":1001}'));
        INSERT INTO demo.${dbName}.variant_nested SELECT
            2,
            named_struct('label', 'second', 'payload', CAST(NULL AS VARIANT)),
            array(parse_json('null'), parse_json('{"kind":"close","score":202}')),
            map('primary', parse_json('{"enabled":false,"score":2002}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_nested_deep;
        CREATE TABLE demo.${dbName}.variant_nested_deep (
            id INT,
            deep STRUCT<
                level1: ARRAY<
                    MAP<STRING, STRUCT<note: STRING, payload: VARIANT>>
                >
            >
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );

        DROP TABLE IF EXISTS demo.${dbName}.variant_nested_legacy_guard;
        CREATE TABLE demo.${dbName}.variant_nested_legacy_guard (
            id INT,
            events ARRAY<VARIANT>
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );

        DROP TABLE IF EXISTS demo.${dbName}.variant_signed_selector;
        CREATE TABLE demo.${dbName}.variant_signed_selector (
            id INT,
            v VARIANT
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_signed_selector
            VALUES (1, parse_json('{"-1":41}'));

        DROP TABLE IF EXISTS demo.${dbName}.variant_evolution;
        CREATE TABLE demo.${dbName}.variant_evolution (
            id INT,
            payload VARIANT,
            note STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version'='3', 'write.format.default'='parquet');
        INSERT INTO demo.${dbName}.variant_evolution
            VALUES (1, parse_json('{"stage":"initial","metric":10}'), 'v1');

        DROP TABLE IF EXISTS demo.${dbName}.variant_write_guard;
        CREATE TABLE demo.${dbName}.variant_write_guard (id INT) USING iceberg
        TBLPROPERTIES ('format-version'='3', 'write.format.default'='parquet');
        INSERT INTO demo.${dbName}.variant_write_guard VALUES (1);

        DROP TABLE IF EXISTS demo.${dbName}.variant_coercion_guard;
        CREATE TABLE demo.${dbName}.variant_coercion_guard (
            id INT,
            payload VARIANT
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false'
        );

        DROP TABLE IF EXISTS demo.${dbName}.variant_operation_column;
        CREATE TABLE demo.${dbName}.variant_operation_column (
            id INT,
            `operation` VARIANT
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='false',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        );
        INSERT INTO demo.${dbName}.variant_operation_column
            VALUES (1, parse_json('{"stage":"spark"}'));
    """

    List<List<Object>> multiFileDataFiles = spark_iceberg """
        SELECT COUNT(*) FROM demo.${dbName}.variant_multi_file.files WHERE content = 0
    """
    assertEquals(1, multiFileDataFiles.size())
    assertTrue(Long.parseLong(multiFileDataFiles[0][0].toString()) > 1,
            "The parallel Variant fixture must contain multiple data files")

    List<List<Object>> multiRowGroupFiles = spark_iceberg """
        SELECT COUNT(*) FROM demo.${dbName}.variant_multi_row_group.files WHERE content = 0
    """
    assertEquals(1, multiRowGroupFiles.size())
    assertEquals("1", multiRowGroupFiles[0][0].toString(),
            "The multi-row-group fixture must contain exactly one data file")

    String equalityDeleteBaseSnapshot = latestSnapshotId("variant_equality_delete")
    String equalityDeleteJava = '''
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

public class AppendVariantEqualityDelete {
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
        Schema equalitySchema = table.schema().select("id");
        int fieldId = table.schema().findField("id").fieldId();
        OutputFile output = table.io().newOutputFile(
                table.location() + "/data/variant-equality-delete-" +
                        System.currentTimeMillis() + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(output)
                .forTable(table)
                .rowSchema(equalitySchema)
                .withSpec(PartitionSpec.unpartitioned())
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(fieldId)
                .overwrite()
                .buildEqualityWriter();
        GenericRecord record = GenericRecord.create(equalitySchema);
        record.setField("id", Integer.valueOf(args[2]));
        writer.write(record);
        writer.close();
        DeleteFile deleteFile = writer.toDeleteFile();
        table.newRowDelta().addDeletes(deleteFile).commit();
    }
}
'''
    String encodedEqualityDeleteJava =
            equalityDeleteJava.getBytes("UTF-8").encodeBase64().toString()
    runInSparkContainer(
            "echo ${encodedEqualityDeleteJava} | base64 -d " +
                    ">/tmp/AppendVariantEqualityDelete.java && " +
                    "javac -cp \"/opt/spark/jars/*\" " +
                    "/tmp/AppendVariantEqualityDelete.java && " +
                    "java -cp \"/tmp:/opt/spark/jars/*\" AppendVariantEqualityDelete " +
                    "${dbName} variant_equality_delete 2")

    String writeGuardSourceSnapshot = latestSnapshotId("variant_write_guard")
    String deletionVectorBaseSnapshot = latestSnapshotId("variant_deletion_vector")
    spark_iceberg """
        ALTER TABLE demo.${dbName}.variant_write_guard ADD COLUMN payload VARIANT
    """
    spark_iceberg """
        DELETE FROM demo.${dbName}.variant_deletion_vector WHERE id % 2 = 1
    """
    List<List<Object>> deletionVectorFiles = spark_iceberg """
        SELECT file_format, content_offset, content_size_in_bytes
        FROM demo.${dbName}.variant_deletion_vector.files
        WHERE content = 1
    """
    assertFalse(deletionVectorFiles.isEmpty(),
            "The Variant deletion fixture must expose a live delete file")
    deletionVectorFiles.each { List<Object> deleteFile ->
        assertEquals("PUFFIN", deleteFile[0].toString().toUpperCase(),
                "The format-v3 Variant fixture must use PUFFIN deletion vectors")
        assertTrue(Long.parseLong(deleteFile[1].toString()) >= 0,
                "A PUFFIN deletion vector must expose its content offset")
        assertTrue(Long.parseLong(deleteFile[2].toString()) > 0,
                "A PUFFIN deletion vector must expose its content size")
    }

    // Register a stable Iceberg metadata fixture so the page-pruning case always uses a
    // standards-compliant shredded Variant file, independent of the Spark writer version.
    minioClient.putObject("warehouse", fixtureKey, shreddedFixture)
    shreddedTableFixture.eachFile { File fixtureFile ->
        minioClient.putObject("warehouse",
                "wh/${dbName}/variant_page_pruning/metadata/${fixtureFile.name}", fixtureFile)
    }
    minioClient.shutdown()
    spark_iceberg """
        CALL demo.system.register_table(
            table => '${dbName}.variant_page_pruning',
            metadata_file =>
                's3a://warehouse/wh/${dbName}/variant_page_pruning/metadata/${shreddedMetadataName}')
    """
    String shreddedOnlySnapshot = latestSnapshotId("variant_page_pruning")
    spark_iceberg_multi """
        ALTER TABLE demo.${dbName}.variant_page_pruning SET TBLPROPERTIES (
            'read.parquet.vectorization.enabled'='false',
            'write.delete.mode'='merge-on-read'
        );
        INSERT INTO demo.${dbName}.variant_page_pruning VALUES
            (5000, parse_json('{"n":5000,"padding":"mixed-unshredded"}'));
    """
    String mixedBeforeDeleteSnapshot = latestSnapshotId("variant_page_pruning")
    // One deletion vector targets the shredded fixture and another targets the appended
    // unshredded file, forcing both physical states through the same scan and delete alignment.
    spark_iceberg """
        DELETE FROM demo.${dbName}.variant_page_pruning WHERE id IN (4095, 5000)
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='${restUri}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'meta.cache.iceberg.table.ttl-second'='0',
            'meta.cache.iceberg.schema.ttl-second'='0'
        )
    """

    sql """switch ${catalogName}"""
    sql """use ${dbName}"""
    sql """set enable_file_scanner_v2=true"""
    sql """set enable_profile=true"""
    sql """set profile_level=2"""

    setFeConfigTemporary([enable_variant_v2: true]) {
        assertTrue(getFeConfig("enable_variant_v2").toBoolean())

        // Doris currently writes only the unshredded metadata/value representation. Reject the
        // table setting instead of silently ignoring the requested shredded encoding.
        String shreddedWriteGuardSnapshot = latestSnapshotId("variant_page_pruning")
        test {
            sql """
                INSERT INTO variant_page_pruning VALUES
                    (6000, PARSE_TO_VARIANT('{"shredding":"unsupported"}'))
            """
            exception "write.parquet.shred-variants=false"
        }
        assertEquals(shreddedWriteGuardSnapshot, latestSnapshotId("variant_page_pruning"),
                "A rejected shredded Variant write must not commit a new snapshot")

        sql """DROP TABLE IF EXISTS variant_doris_write_v2"""
        test {
            sql """
                CREATE TABLE variant_doris_write_v2 (
                    id INT,
                    payload VARIANT
                ) ENGINE=ICEBERG
                PROPERTIES ('format-version'='2', 'write.format.default'='parquet')
            """
            exception "Iceberg VARIANT writes require table format-version 3"
        }
        sql """DROP TABLE IF EXISTS variant_doris_write_orc"""
        test {
            sql """
                CREATE TABLE variant_doris_write_orc (
                    id INT,
                    payload VARIANT
                ) ENGINE=ICEBERG
                PROPERTIES ('format-version'='3', 'write.format.default'='orc')
            """
            exception "Iceberg VARIANT writes require Parquet data files"
        }
        sql """DROP TABLE IF EXISTS variant_doris_nested_ddl_guard"""
        test {
            sql """
                CREATE TABLE variant_doris_nested_ddl_guard (
                    id INT,
                    info STRUCT<label: STRING, payload: VARIANT>
                ) ENGINE=ICEBERG
                PROPERTIES ('format-version'='3', 'write.format.default'='parquet')
            """
            exception "STRUCT unsupported sub-type: variant"
        }

        sql """DROP TABLE IF EXISTS variant_doris_write"""
        sql """
            CREATE TABLE variant_doris_write (
                id INT,
                payload VARIANT
            ) ENGINE=ICEBERG
            PROPERTIES ('format-version'='3', 'write.format.default'='parquet')
        """
        // Common-type analysis runs before sink binding. Reject implicit Variant-to-scalar casts
        // instead of persisting an object/array as SQL NULL, while keeping ordinary scalar writes.
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT id, v FROM variant_values WHERE id = 1
                UNION ALL
                SELECT 2, 1
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT id + 2, v FROM variant_root_arrays WHERE id = 1
                UNION ALL
                SELECT 4, 1
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT 5, IF(TRUE, v, 1) FROM variant_values WHERE id = 1
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT 6, IF(TRUE, v, 1) FROM variant_root_arrays WHERE id = 2
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT 7, CASE WHEN TRUE THEN v ELSE 1 END
                FROM variant_values WHERE id = 1
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT 8, CASE WHEN TRUE THEN v ELSE 1 END
                FROM variant_root_arrays WHERE id = 2
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                WITH RECURSIVE source AS (
                    SELECT IF(TRUE, v, 1) AS payload FROM variant_values WHERE id = 1
                    UNION ALL
                    SELECT CAST(1 AS DECIMAL(38, 9)) FROM source
                )
                SELECT 9, payload FROM source
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }
        test {
            sql """
                INSERT INTO variant_coercion_guard
                SELECT 9, generated_payload
                FROM (SELECT v FROM variant_values WHERE id = 1) source
                LATERAL VIEW explode(ARRAY(IF(TRUE, v, 1))) generated AS generated_payload
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'payload'"
        }

        // Primitive-to-Variant coercion remains supported for both inline VALUES and ordinary SELECT.
        sql """
            INSERT INTO variant_coercion_guard VALUES
                (10, 7),
                (11, 'values-string'),
                (12, TRUE),
                (13, ARRAY(1, 2))
        """
        sql """INSERT INTO variant_coercion_guard SELECT 20, 8"""
        sql """INSERT INTO variant_coercion_guard SELECT 21, 'select-string'"""
        sql """INSERT INTO variant_coercion_guard SELECT 22, FALSE"""
        sql """INSERT INTO variant_coercion_guard SELECT 23, ARRAY(3, 4)"""
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_coercion_guard"""
        List<List<Object>> sparkCoercionGuardRows = spark_iceberg """
            SELECT id, to_json(payload)
            FROM demo.${dbName}.variant_coercion_guard
            ORDER BY id
        """
        assertEquals([
                ["10", "7"],
                ["11", '"values-string"'],
                ["12", "true"],
                ["13", "[1,2]"],
                ["20", "8"],
                ["21", '"select-string"'],
                ["22", "false"],
                ["23", "[3,4]"]
        ], sparkCoercionGuardRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        sql """
            INSERT INTO variant_doris_write VALUES
                (1, PARSE_TO_VARIANT('{"name":"doris","n":20,"enabled":true,"ratio":1.25,"nested":{"city":"hangzhou"},"tags":["iceberg","spark"]}')),
                (2, PARSE_TO_VARIANT('{"name":"second","n":-7,"enabled":false,"ratio":-3.5,"nested":{"city":"shanghai"},"tags":["doris","variant"]}')),
                (3, PARSE_TO_VARIANT('{"name":"unicode-中文","n":0,"enabled":true,"ratio":0.0,"nested":{"city":null},"tags":[]}')),
                (4, PARSE_TO_VARIANT('[1,"two",null,{"k":4}]')),
                (5, PARSE_TO_VARIANT('42')),
                (6, PARSE_TO_VARIANT('"root-string"')),
                (7, PARSE_TO_VARIANT('true')),
                (8, PARSE_TO_VARIANT('null')),
                (9, NULL),
                (10, PARSE_TO_VARIANT('"null"')),
                (11, PARSE_TO_VARIANT('""')),
                (12, PARSE_TO_VARIANT('{}')),
                (13, PARSE_TO_VARIANT('[]'))
        """

        // A single Doris writer emits more than 128 MiB of logical data, forcing multiple Parquet
        // row groups in one file. The nested Variant leaf verifies that Iceberg metrics are merged
        // across every row group instead of taking statistics from only the first one.
        sql """
            INSERT INTO variant_doris_multi_row_group
            SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */
                   number,
                   NAMED_STRUCT(
                       'label', 'doris-row-group',
                       'payload', IF(
                           number % 10 = 0,
                           CAST(NULL AS VARIANT),
                           PARSE_TO_VARIANT(CONCAT('{"kind":"doris","n":', number, '}'))
                       )
                   ),
                   REPEAT('x', 2048)
            FROM numbers("number" = "100000")
        """
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_doris_multi_row_group"""
        // Iceberg allocates IDs to all top-level fields before nested fields; payload is 5, not 4.
        int nestedPayloadFieldId = 5
        List<List<Object>> sparkDorisMultiRowGroupFiles = spark_iceberg """
            SELECT file_format, record_count,
                   value_counts[${nestedPayloadFieldId}],
                   null_value_counts[${nestedPayloadFieldId}],
                   column_sizes[${nestedPayloadFieldId}] IS NULL,
                   lower_bounds[${nestedPayloadFieldId}] IS NULL,
                   upper_bounds[${nestedPayloadFieldId}] IS NULL
            FROM demo.${dbName}.variant_doris_multi_row_group.files
            WHERE content = 0
        """
        assertEquals(1, sparkDorisMultiRowGroupFiles.size(),
                "The Doris multi-row-group write must produce exactly one data file")
        assertEquals([
                "parquet", "100000", "100000", "10000", "true", "true", "true"
        ], sparkDorisMultiRowGroupFiles[0].collect { value ->
            value == null ? null : value.toString().toLowerCase()
        })
        List<List<Object>> sparkDorisMultiRowGroupValues = spark_iceberg """
            SELECT COUNT(*), COUNT(info.payload),
                   SUM(CASE WHEN info.payload IS NULL THEN 1 ELSE 0 END),
                   MIN(id), MAX(id), MIN(LENGTH(padding)), MAX(LENGTH(padding))
            FROM demo.${dbName}.variant_doris_multi_row_group
        """
        assertEquals([
                "100000", "90000", "10000", "0", "99999", "2048", "2048"
        ], sparkDorisMultiRowGroupValues[0].collect { value -> value.toString() })
        List<List<Object>> sparkDorisMultiRowGroupSamples = spark_iceberg """
            SELECT id, info.label, variant_get(info.payload, '\$.n', 'bigint')
            FROM demo.${dbName}.variant_doris_multi_row_group
            WHERE id IN (0, 1, 99999)
            ORDER BY id
        """
        assertEquals([
                ["0", "doris-row-group", null],
                ["1", "doris-row-group", "1"],
                ["99999", "doris-row-group", "99999"]
        ], sparkDorisMultiRowGroupSamples.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })
        // CREATE/ADD followed by top-level MODIFY must remain metadata-only for Iceberg Variant.
        sql """DROP TABLE IF EXISTS variant_doris_modify"""
        sql """
            CREATE TABLE variant_doris_modify (
                id INT,
                created_payload VARIANT NOT NULL
            ) ENGINE=ICEBERG
            PROPERTIES ('format-version'='3', 'write.format.default'='parquet')
        """
        sql """
            ALTER TABLE variant_doris_modify
            MODIFY COLUMN created_payload VARIANT NULL COMMENT 'created variant'
        """
        sql """ALTER TABLE variant_doris_modify ADD COLUMN added_payload VARIANT"""
        sql """
            ALTER TABLE variant_doris_modify
            MODIFY COLUMN added_payload VARIANT COMMENT 'added variant' FIRST
        """
        sql """
            INSERT INTO variant_doris_modify (id, created_payload, added_payload)
            VALUES (1, PARSE_TO_VARIANT('{"source":"create"}'),
                    PARSE_TO_VARIANT('{"source":"add"}'))
        """
        List<List<Object>> sparkModifiedVariantRows = spark_iceberg """
            SELECT id,
                   variant_get(created_payload, '\$.source', 'string'),
                   variant_get(added_payload, '\$.source', 'string')
            FROM demo.${dbName}.variant_doris_modify
        """
        assertEquals([["1", "create", "add"]], sparkModifiedVariantRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })
        List<List<Object>> sparkDorisVariantRows = spark_iceberg """
            SELECT id,
                   variant_get(payload, '\$.name', 'string'),
                   variant_get(payload, '\$.n', 'int'),
                   variant_get(payload, '\$.enabled', 'boolean'),
                   variant_get(payload, '\$.ratio', 'double'),
                   variant_get(payload, '\$.nested.city', 'string'),
                   variant_get(payload, '\$.tags[1]', 'string'),
                   variant_get(payload, '\$[0]', 'int'),
                   variant_get(payload, '\$[1]', 'string'),
                   variant_get(payload, '\$[3].k', 'int'),
                   payload IS NULL
            FROM demo.${dbName}.variant_doris_write
            ORDER BY id
        """
        List<List<String>> sparkDorisVariantStrings = sparkDorisVariantRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString().toLowerCase() }
        }
        assertEquals([
                ["1", "doris", "20", "true", "1.25", "hangzhou", "spark",
                        null, null, null, "false"],
                ["2", "second", "-7", "false", "-3.5", "shanghai", "variant",
                        null, null, null, "false"],
                ["3", "unicode-中文", "0", "true", "0.0", null, null,
                        null, null, null, "false"],
                ["4", null, null, null, null, null, null, "1", "two", "4", "false"],
                ["5", null, null, null, null, null, null, null, null, null, "false"],
                ["6", null, null, null, null, null, null, null, null, null, "false"],
                ["7", null, null, null, null, null, null, null, null, null, "false"],
                ["8", null, null, null, null, null, null, null, null, null, "false"],
                ["9", null, null, null, null, null, null, null, null, null, "true"],
                ["10", null, null, null, null, null, null, null, null, null, "false"],
                ["11", null, null, null, null, null, null, null, null, null, "false"],
                ["12", null, null, null, null, null, null, null, null, null, "false"],
                ["13", null, null, null, null, null, null, null, null, null, "false"]
        ], sparkDorisVariantStrings)

        // Keep SQL NULL, Variant null, the string "null", and empty values distinct.
        List<List<Object>> sparkDorisRootRows = spark_iceberg """
            SELECT id, to_json(payload), payload IS NULL
            FROM demo.${dbName}.variant_doris_write
            WHERE id BETWEEN 5 AND 13
            ORDER BY id
        """
        assertEquals([
                ["5", "42", "false"],
                ["6", '"root-string"', "false"],
                ["7", "true", "false"],
                ["8", "null", "false"],
                ["9", null, "true"],
                ["10", '"null"', "false"],
                ["11", '""', "false"],
                ["12", "{}", "false"],
                ["13", "[]", "false"]
        ], sparkDorisRootRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        List<List<Object>> sparkDorisVariantFiles = spark_iceberg """
            SELECT file_format, record_count,
                   value_counts[2], null_value_counts[2],
                   lower_bounds[2] IS NULL, upper_bounds[2] IS NULL
            FROM demo.${dbName}.variant_doris_write.files
            WHERE content = 0
        """
        assertTrue(!sparkDorisVariantFiles.isEmpty())
        assertTrue(sparkDorisVariantFiles.every { row -> row[0].toString().equalsIgnoreCase("parquet") })
        assertEquals(13L, sparkDorisVariantFiles.sum { row -> Long.parseLong(row[1].toString()) })
        assertEquals(13L, sparkDorisVariantFiles.sum { row -> Long.parseLong(row[2].toString()) })
        assertEquals(1L, sparkDorisVariantFiles.sum { row -> Long.parseLong(row[3].toString()) })
        assertTrue(sparkDorisVariantFiles.every { row -> row[4].toString().equalsIgnoreCase("true") })
        assertTrue(sparkDorisVariantFiles.every { row -> row[5].toString().equalsIgnoreCase("true") })

        // Doris writes Variant V2 leaves nested in every Iceberg complex container. Spark validates
        // the committed Parquet Variant values, including SQL NULL versus Variant null and empties.
        sql """
            INSERT INTO variant_nested VALUES
                (
                    3,
                    NAMED_STRUCT(
                        'label', 'doris-nested',
                        'payload', PARSE_TO_VARIANT('{"kind":"struct","n":3}')
                    ),
                    ARRAY(
                        PARSE_TO_VARIANT('null'),
                        CAST(NULL AS VARIANT),
                        PARSE_TO_VARIANT('"null"'),
                        PARSE_TO_VARIANT('{}')
                    ),
                    MAP(
                        'object', PARSE_TO_VARIANT('{"kind":"map","n":30}'),
                        'json_null', PARSE_TO_VARIANT('null'),
                        'sql_null', CAST(NULL AS VARIANT)
                    )
                ),
                (4, NAMED_STRUCT('label', 'empty', 'payload', PARSE_TO_VARIANT('{}')),
                        ARRAY(), MAP()),
                (5, NULL, NULL, NULL)
        """
        // Spark caches Iceberg snapshots, so refresh after a Doris commit before cross-engine reads.
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_nested"""

        List<List<Object>> sparkNestedContainers = spark_iceberg """
            SELECT id,
                   info.label,
                   variant_get(info.payload, '\$.kind', 'string'),
                   variant_get(info.payload, '\$.n', 'int'),
                   info.payload IS NULL,
                   events IS NULL, size(events),
                   attrs IS NULL, size(attrs)
            FROM demo.${dbName}.variant_nested
            WHERE id IN (3, 4, 5)
            ORDER BY id
        """
        assertEquals([
                ["3", "doris-nested", "struct", "3", "false", "false", "4", "false", "3"],
                ["4", "empty", null, null, "false", "false", "0", "false", "0"],
                ["5", null, null, null, "true", "true", null, "true", null]
        ], sparkNestedContainers.collect { row ->
            row.collect { value -> value == null ? null : value.toString().toLowerCase() }
        })

        // Iceberg 1.10.1 cannot materialize Variant values from ArrayData/MapData in Spark 4.
        // Doris still validates every nested value written to the shared Parquet files.
        List<List<Object>> dorisNestedVariantValues = sql """
            SELECT id,
                   CAST(info.payload['kind'] AS STRING),
                   CAST(info.payload['n'] AS INT),
                   VARIANT_TYPE(events[1]),
                   events[2] IS NULL,
                   VARIANT_TYPE(events[3]),
                   CAST(events[3] AS STRING),
                   VARIANT_TYPE(events[4]),
                   CAST(attrs['object']['kind'] AS STRING),
                   CAST(attrs['object']['n'] AS INT),
                   VARIANT_TYPE(attrs['json_null']),
                   attrs['sql_null'] IS NULL
            FROM variant_nested
            WHERE id = 3
        """
        assertEquals([[
                "3", "struct", "3", "null", "true", "string", "null", "object",
                "map", "30", "null", "true"
        ]], dorisNestedVariantValues.collect { row ->
            row.collect { value -> value == null ? null : value.toString().toLowerCase() }
        })

        sql """
            INSERT INTO variant_nested_deep VALUES
                (
                    1,
                    NAMED_STRUCT(
                        'level1',
                        ARRAY(
                            MAP(
                                'outer',
                                NAMED_STRUCT(
                                    'note', 'from-doris',
                                    'payload', PARSE_TO_VARIANT(
                                        '{"level2":{"level3":{"value":"deep-ok"}}}')
                                )
                            )
                        )
                    )
                ),
                (
                    2,
                    NAMED_STRUCT(
                        'level1',
                        ARRAY(MAP(
                            'outer',
                            NAMED_STRUCT('note', 'sql-null', 'payload', CAST(NULL AS VARIANT))
                        ))
                    )
                )
        """
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_nested_deep"""
        List<List<Object>> sparkDeepContainerRows = spark_iceberg """
            SELECT id,
                   deep.level1[0]['outer'].note,
                   size(deep.level1)
            FROM demo.${dbName}.variant_nested_deep
            ORDER BY id
        """
        assertEquals([
                ["1", "from-doris", "1"],
                ["2", "sql-null", "1"]
        ], sparkDeepContainerRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString().toLowerCase() }
        })
        List<List<Object>> dorisDeepVariantRows = sql """
            SELECT id,
                   deep.level1[1]['outer'].note,
                   CAST(deep.level1[1]['outer'].payload['level2']['level3']['value'] AS STRING),
                   deep.level1[1]['outer'].payload IS NULL
            FROM variant_nested_deep
            ORDER BY id
        """
        assertEquals([
                ["1", "from-doris", "deep-ok", "false"],
                ["2", "sql-null", null, "true"]
        ], dorisDeepVariantRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString().toLowerCase() }
        })

        // The first operation slot is merge-routing metadata; a quoted user column with the same
        // name remains an ordinary data column and must still receive Variant V2 validation/coercion.
        sql """
            UPDATE variant_operation_column
            SET `operation` = PARSE_TO_VARIANT('{"stage":"doris-update","n":1}')
            WHERE id = 1
        """
        sql """
            MERGE INTO variant_operation_column t
            USING (
                SELECT 1 AS id, PARSE_TO_VARIANT('{"stage":"doris-merge","n":2}') AS payload
                UNION ALL
                SELECT 2 AS id, PARSE_TO_VARIANT('{"stage":"doris-insert","n":3}') AS payload
            ) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET `operation` = s.payload
            WHEN NOT MATCHED THEN INSERT (id, `operation`) VALUES (s.id, s.payload)
        """
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_operation_column"""
        List<List<Object>> sparkOperationRows = spark_iceberg """
            SELECT id,
                   variant_get(`operation`, '\$.stage', 'string'),
                   variant_get(`operation`, '\$.n', 'int')
            FROM demo.${dbName}.variant_operation_column
            ORDER BY id
        """
        assertEquals([
                ["1", "doris-merge", "2"],
                ["2", "doris-insert", "3"]
        ], sparkOperationRows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        // Row-level DML must inspect the complete source lineage, not only the final sink-child type.
        // Otherwise common-type coercion can turn an object Variant into SQL NULL before it is encoded.
        test {
            sql """
                UPDATE variant_operation_column
                SET `operation` = IF(TRUE, `operation`, 1)
                WHERE id = 1
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'operation'"
        }
        test {
            sql """
                MERGE INTO variant_operation_column t
                USING (
                    SELECT 1 AS id, PARSE_TO_VARIANT('{"stage":"lossy-merge"}') AS payload
                    UNION ALL
                    SELECT 2 AS id, 1 AS payload
                ) s
                ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET `operation` = s.payload
            """
            exception "Iceberg VARIANT write cannot safely convert input column 'operation'"
        }
        List<List<Object>> sparkOperationRowsAfterRejectedDml = spark_iceberg """
            SELECT id,
                   variant_get(`operation`, '\$.stage', 'string'),
                   variant_get(`operation`, '\$.n', 'int')
            FROM demo.${dbName}.variant_operation_column
            ORDER BY id
        """
        assertEquals([
                ["1", "doris-merge", "2"],
                ["2", "doris-insert", "3"]
        ], sparkOperationRowsAfterRejectedDml.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })

        // Coerce each MERGE action to the Iceberg target before building the action IF. Otherwise
        // the unchanged object Variant and primitive insert value may first be coerced to DECIMAL,
        // turning the object into SQL NULL before the sink sees it.
        sql """
            MERGE INTO variant_doris_write t
            USING (
                SELECT 1 AS id
                UNION ALL
                SELECT 14 AS id
            ) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET payload = t.payload
            WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (s.id, 1)
        """
        spark_iceberg """REFRESH TABLE demo.${dbName}.variant_doris_write"""
        List<List<Object>> sparkUnchangedMergeVariant = spark_iceberg """
            SELECT variant_get(payload, '\$.name', 'string'),
                   variant_get(payload, '\$.n', 'int'),
                   variant_get(payload, '\$.nested.city', 'string')
            FROM demo.${dbName}.variant_doris_write
            WHERE id = 1
        """
        assertEquals([["doris", "20", "hangzhou"]], sparkUnchangedMergeVariant.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        })
        List<List<Object>> sparkPrimitiveMergeVariant = spark_iceberg """
            SELECT to_json(payload)
            FROM demo.${dbName}.variant_doris_write
            WHERE id = 14
        """
        assertEquals("1", sparkPrimitiveMergeVariant[0][0].toString())

        // With native Variant V2 disabled, an OLAP VARIANT column uses the legacy physical
        // representation. Do not silently convert it for an Iceberg Variant V2 sink.
        setFeConfigTemporary([enable_variant_v2: false]) {
            assertFalse(getFeConfig("enable_variant_v2").toBoolean())

            String internalVariantDbName = "iceberg_variant_write_internal_db"
            sql """DROP DATABASE IF EXISTS internal.${internalVariantDbName} FORCE"""
            sql """CREATE DATABASE internal.${internalVariantDbName}"""
            try {
                sql """
                    CREATE TABLE internal.${internalVariantDbName}.variant_source (
                        id INT,
                        payload VARIANT
                    ) ENGINE=OLAP
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """
                // Let the sink build the parameterized legacy Variant used by the target column.
                sql """
                    INSERT INTO internal.${internalVariantDbName}.variant_source VALUES
                        (1, '{"name":"legacy"}')
                """

                sql """DROP TABLE IF EXISTS variant_doris_legacy_write"""
                sql """
                    CREATE TABLE variant_doris_legacy_write (
                        id INT,
                        payload VARIANT
                    ) ENGINE=ICEBERG
                    PROPERTIES ('format-version'='3', 'write.format.default'='parquet')
                """
                test {
                    sql """
                        INSERT INTO variant_doris_legacy_write
                        SELECT id, payload
                        FROM internal.${internalVariantDbName}.variant_source
                    """
                    exception "Writing legacy Doris VARIANT to Iceberg VARIANT column 'payload' is not supported"
                }

                List<List<Object>> sparkLegacyVariantRows = spark_iceberg """
                    SELECT COUNT(*) FROM demo.${dbName}.variant_doris_legacy_write
                """
                assertEquals("0", sparkLegacyVariantRows[0][0].toString())

                // Legacy execution rejects nested Variant containers before Iceberg sink analysis.
                test {
                    sql """
                        INSERT INTO variant_nested_legacy_guard
                        SELECT id, ARRAY(payload)
                        FROM internal.${internalVariantDbName}.variant_source
                    """
                    exception "array does not support jsonb/variant type"
                }
                List<List<Object>> sparkLegacyNestedRows = spark_iceberg """
                    SELECT COUNT(*) FROM demo.${dbName}.variant_nested_legacy_guard
                """
                assertEquals("0", sparkLegacyNestedRows[0][0].toString())

                // MERGE materializes the compute-only target before sink-specific validation.
                test {
                    sql """
                        MERGE INTO variant_operation_column t
                        USING internal.${internalVariantDbName}.variant_source s
                        ON t.id = s.id
                        WHEN MATCHED THEN UPDATE SET `operation` = s.payload
                    """
                    exception "Cast between legacy Variant and compute-only Variant V2 is not supported"
                }
                List<List<Object>> sparkOperationAfterRejectedMerge = spark_iceberg """
                    SELECT variant_get(`operation`, '\$.stage', 'string')
                    FROM demo.${dbName}.variant_operation_column
                    WHERE id = 1
                """
                assertEquals("doris-merge", sparkOperationAfterRejectedMerge[0][0].toString())
            } finally {
                sql """DROP DATABASE IF EXISTS internal.${internalVariantDbName} FORCE"""
            }
        }
    }

    def profileAction = new ProfileAction(context)
    def mergedProfile = { String profile ->
        if (!profile.contains("MergedProfile:")) {
            return profile
        }
        String merged = profile.substring(profile.indexOf("MergedProfile:"))
        int end = merged.length()
        ["DetailProfile(", "Execution Profile:", "Appendix:"].each { String sectionName ->
            int sectionIndex = merged.indexOf(sectionName)
            if (sectionIndex > 0) {
                end = Math.min(end, sectionIndex)
            }
        }
        return merged.substring(0, end)
    }
    def counterSum = { String profile, String counterName ->
        Pattern pattern = Pattern.compile("(?m)^\\s*(?:-\\s*)?" +
                Pattern.quote(counterName) + ":\\s+([^\\n]+)")
        Matcher matcher = pattern.matcher(mergedProfile(profile))
        long sum = 0
        while (matcher.find()) {
            String valueText = matcher.group(1)
            // Merged counters may be human-readable; the parenthesized value is the exact sum.
            Matcher exact = Pattern.compile("\\(([0-9,]+)\\)").matcher(valueText)
            Matcher number = Pattern.compile("([0-9,]+)").matcher(valueText)
            if (exact.find()) {
                sum += Long.parseLong(exact.group(1).replace(",", ""))
            } else if (number.find()) {
                sum += Long.parseLong(number.group(1).replace(",", ""))
            }
        }
        return sum
    }
    def getProfileByToken = { String token, List<String> positiveCounters = [] ->
        String lastProfile = profileAction.getProfileBySql(token, positiveCounters)
        if (positiveCounters.every { String counter -> counterSum(lastProfile, counter) > 0 }) {
            return lastProfile
        }
        return profileAction.waitProfile({
            lastProfile = profileAction.getProfileBySql(token, positiveCounters)
            return positiveCounters.every {
                String counter -> counterSum(lastProfile, counter) > 0
            } ? lastProfile : ""
        }, [], "Completed profile with positive counters ${positiveCounters} for ${token}")
    }

    String evolutionInitial = latestSnapshotId("variant_evolution")
    sql """ALTER TABLE variant_evolution CREATE TAG variant_initial"""

    order_qt_variant_evolution_initial_snapshot """
        SELECT id, CAST(payload['stage'] AS STRING), CAST(payload['metric'] AS INT), note
        FROM variant_evolution FOR VERSION AS OF ${evolutionInitial}
        ORDER BY id
    """

    order_qt_variant_evolution_initial_tag """
        SELECT id, CAST(payload['stage'] AS STRING), CAST(payload['metric'] AS INT), note
        FROM variant_evolution FOR VERSION AS OF 'variant_initial'
        ORDER BY id
    """

    List<List<Object>> initialTime = sql """
        SELECT date_format(date_add(committed_at, interval 1 second), '%Y-%m-%d %H:%i:%s')
        FROM variant_evolution\$snapshots
        WHERE snapshot_id = ${evolutionInitial}
    """
    assertEquals(1, initialTime.size())
    order_qt_variant_evolution_initial_time """
        SELECT id, CAST(payload['stage'] AS STRING), CAST(payload['metric'] AS INT), note
        FROM variant_evolution FOR TIME AS OF "${initialTime[0][0]}"
        ORDER BY id
    """

    spark_iceberg_multi """
        ALTER TABLE demo.${dbName}.variant_evolution RENAME COLUMN payload TO event;
        INSERT INTO demo.${dbName}.variant_evolution
            VALUES (2, parse_json('{"stage":"renamed","metric":20}'), 'v2');
    """
    String evolutionRenamed = latestSnapshotId("variant_evolution")
    sql """ALTER TABLE variant_evolution CREATE TAG variant_renamed"""

    spark_iceberg_multi """
        ALTER TABLE demo.${dbName}.variant_evolution ADD COLUMN aux VARIANT;
        ALTER TABLE demo.${dbName}.variant_evolution ALTER COLUMN aux FIRST;
        INSERT INTO demo.${dbName}.variant_evolution (id, event, note, aux)
            VALUES (3, parse_json('{"stage":"with-aux","metric":30}'), 'v3',
                    parse_json('{"side":300}'));
    """
    String evolutionWithAux = latestSnapshotId("variant_evolution")

    spark_iceberg_multi """
        ALTER TABLE demo.${dbName}.variant_evolution RENAME COLUMN aux TO sidecar;
        ALTER TABLE demo.${dbName}.variant_evolution DROP COLUMN event;
        INSERT INTO demo.${dbName}.variant_evolution (id, note, sidecar)
            VALUES (4, 'v4', parse_json('{"side":400}'));
    """
    String evolutionDropped = latestSnapshotId("variant_evolution")

    spark_iceberg_multi """
        ALTER TABLE demo.${dbName}.variant_evolution ADD COLUMN event VARIANT;
        INSERT INTO demo.${dbName}.variant_evolution (id, note, sidecar, event)
            VALUES (5, 'v5', parse_json('{"side":500}'),
                    parse_json('{"stage":"readded","metric":50}'));

        -- Write ORC before evolving the logical schema to Variant. This retains valid ORC files
        -- in the snapshots while using Iceberg FileIO instead of Spark native ORC, whose optional
        -- S3A implementation may not be installed.
        DROP TABLE IF EXISTS demo.${dbName}.variant_orc;
        CREATE TABLE demo.${dbName}.variant_orc (id INT) USING iceberg
        TBLPROPERTIES ('format-version'='3', 'write.format.default'='orc');
        INSERT INTO demo.${dbName}.variant_orc VALUES (1);
        ALTER TABLE demo.${dbName}.variant_orc ADD COLUMN v VARIANT;

        DROP TABLE IF EXISTS demo.${dbName}.variant_mixed_format;
        CREATE TABLE demo.${dbName}.variant_mixed_format (id INT) USING iceberg
        TBLPROPERTIES ('format-version'='3', 'write.format.default'='orc');
        INSERT INTO demo.${dbName}.variant_mixed_format VALUES (2);
        ALTER TABLE demo.${dbName}.variant_mixed_format ADD COLUMN v VARIANT;
        ALTER TABLE demo.${dbName}.variant_mixed_format SET TBLPROPERTIES
            ('write.format.default'='parquet');
        INSERT INTO demo.${dbName}.variant_mixed_format
            VALUES (1, parse_json('{"format":"parquet"}'));
    """
    String evolutionReadded = latestSnapshotId("variant_evolution")

    // Root projection covers objects, arrays, scalars, Variant null and SQL NULL.
    order_qt_variant_root_projection """
        SELECT id, v IS NULL, CAST(v AS STRING)
        FROM variant_values
        ORDER BY id
    """

    order_qt_variant_root_array_projection """
        SELECT id,
               v IS NULL,
               CAST(v AS STRING),
               CAST(v[1] AS STRING),
               CAST(v[2] AS INT),
               CAST(v[3]['x'] AS INT),
               CAST(v[4][2] AS INT)
        FROM variant_root_arrays
        ORDER BY id
    """

    order_qt_variant_path_expressions """
        SELECT id,
               UPPER(CAST(v['name'] AS STRING)),
               CAST(v['n'] AS INT) + 1,
               ROUND(CAST(v['ratio'] AS DOUBLE), 1),
               CAST(v['ok'] AS BOOLEAN),
               ARRAY_SUM(CAST(v['arr'] AS ARRAY<INT>)),
               CAST(v['nested']['city'] AS STRING)
        FROM variant_values
        WHERE id IN (1, 2, 9, 10, 11)
        ORDER BY id
    """

    order_qt_variant_deep_object_fallback """
        SELECT id,
               CAST(v['profile']['address']['country']['code'] AS STRING),
               CAST(v['profile']['address']['country']['rank'] AS INT)
        FROM variant_deep_object
        ORDER BY id
    """

    order_qt_variant_filter """
        SELECT id, CAST(v['name'] AS STRING), CAST(v['n'] AS INT)
        FROM variant_values
        WHERE CAST(v['n'] AS INT) >= 20
          AND CAST(v['ok'] AS BOOLEAN) = true
        ORDER BY id
    """

    // Keep the independent Spark writes on one scanner to exercise metadata dictionaries and
    // complete Variant state transitions across file boundaries before batching.
    sql "set parallel_pipeline_task_num=1"
    sql "set max_file_scanners_concurrency=1"
    order_qt_variant_cross_file_leaf_projection """
        SELECT id, CAST(v['n'] AS INT)
        FROM variant_values
        ORDER BY id
    """

    order_qt_variant_multi_file_serial """
        SELECT id,
               CAST(v['shared'] AS INT),
               CAST(v['a'] AS INT),
               CAST(v['b'] AS INT),
               CAST(v['new_field']['k'] AS INT),
               CAST(v AS STRING)
        FROM variant_multi_file
        WHERE v['shared'] >= 20
        ORDER BY id
    """
    sql "set parallel_pipeline_task_num=4"
    sql "set max_file_scanners_concurrency=8"
    sql "set min_file_scanners_concurrency=4"
    // Scanner tasks pull file ranges dynamically, so minimum concurrency does not guarantee that
    // every scheduled scanner consumes rows. Validate parallel correctness without pinning the
    // scheduler's nondeterministic range assignment.
    order_qt_variant_multi_file_parallel """
        SELECT id,
               CAST(v['shared'] AS INT),
               CAST(v['a'] AS INT),
               CAST(v['b'] AS INT),
               CAST(v['new_field']['k'] AS INT),
               CAST(v AS STRING)
        FROM variant_multi_file
        WHERE v['shared'] >= 20
        ORDER BY id
    """
    // The stable snapshot contributes a genuinely shredded file, while the appended file uses
    // the unshredded fallback. More than four rows qualify, forcing local TopN overshoot to be
    // truncated after the merge exchange while the mapper-eligible projected path crosses the wire.
    explain {
        sql """
            SELECT id, CAST(projected['n'] AS INT)
            FROM (
                SELECT id, v AS projected
                FROM variant_page_pruning FOR VERSION AS OF ${mixedBeforeDeleteSnapshot}
                WHERE CAST(v['n'] AS INT) > 3000
                ORDER BY id DESC
                LIMIT 4
            ) gathered
        """
        contains "VMERGING-EXCHANGE"
        contains "inputSplitNum=2"
        contains "all access paths: [v(2).n]"
    }
    String projectedGatherToken =
            "iceberg_variant_projected_remote_gather_" + UUID.randomUUID().toString()
    List<List<Object>> projectedGatherRows = sql """
        SELECT '${projectedGatherToken}', id, CAST(projected['n'] AS INT)
        FROM (
            SELECT id, v AS projected
            FROM variant_page_pruning FOR VERSION AS OF ${mixedBeforeDeleteSnapshot}
            WHERE CAST(v['n'] AS INT) > 3000
            ORDER BY id DESC
            LIMIT 4
        ) gathered
        ORDER BY id
    """
    assertEquals(4, projectedGatherRows.size())
    String projectedGatherProfile = getProfileByToken(projectedGatherToken,
            ["VariantLeafProjections", "VariantDirectLeafPathMisses"]).toString()
    assertTrue(counterSum(projectedGatherProfile, "VariantLeafProjections") > 0,
            "The projected TopN did not read a physical shredded Variant leaf")
    assertTrue(counterSum(projectedGatherProfile, "VariantDirectLeafPathMisses") > 0,
            "The projected TopN did not combine the unshredded fallback file")
    order_qt_variant_projected_remote_gather """
        SELECT id,
               CAST(projected['n'] AS INT)
        FROM (
            SELECT id, v AS projected
            FROM variant_page_pruning FOR VERSION AS OF ${mixedBeforeDeleteSnapshot}
            WHERE CAST(v['n'] AS INT) > 3000
            ORDER BY id DESC
            LIMIT 4
        ) gathered
        ORDER BY id
    """
    sql "set min_file_scanners_concurrency=1"

    order_qt_variant_type_matrix """
        SELECT CAST(v['bool_value'] AS BOOLEAN),
               CAST(v['tiny_value'] AS TINYINT),
               CAST(v['small_value'] AS SMALLINT),
               CAST(v['int_value'] AS INT),
               CAST(v['big_value'] AS BIGINT),
               ISNAN(CAST(v['float_value'] AS FLOAT)),
               ISINF(CAST(v['double_value'] AS DOUBLE)),
               CAST(v['decimal_value'] AS DECIMAL(20, 4)),
               CAST(v['date_value'] AS DATE),
               CAST(v['timestamp_value'] AS DATETIMEV2(6)),
               CAST(v['binary_value'] AS STRING),
               v['null_value'] IS NULL
        FROM variant_type_matrix
    """

    String dorisMultiRowGroupToken =
            "iceberg_variant_doris_multi_row_group_" + UUID.randomUUID().toString()
    List<List<Object>> dorisMultiRowGroupRows = sql """
        SELECT '${dorisMultiRowGroupToken}', COUNT(*), SUM(LENGTH(padding)),
               SUM(IF(info.payload IS NULL, 1, 0))
        FROM variant_doris_multi_row_group
    """
    assertEquals([
            dorisMultiRowGroupToken, "100000", "204800000", "10000"
    ], dorisMultiRowGroupRows[0].collect { value -> value.toString() })
    String dorisMultiRowGroupProfile = getProfileByToken(
            dorisMultiRowGroupToken, ["RowGroupsTotalNum"]).toString()
    assertTrue(counterSum(dorisMultiRowGroupProfile, "RowGroupsTotalNum") > 1,
            "The single Doris-generated Parquet file did not contain multiple row groups")

    String multiRowGroupColdToken =
            "iceberg_variant_multi_row_group_cold_" + UUID.randomUUID().toString()
    sql """
        SELECT '${multiRowGroupColdToken}', COUNT(*), MIN(id), MAX(id)
        FROM variant_multi_row_group
        WHERE CAST(v['n'] AS INT) >= 8000
    """
    String multiRowGroupColdProfile = getProfileByToken(multiRowGroupColdToken,
            ["RowGroupsTotalNum", "VariantDirectLeafPathMisses", "VariantReconstructedRows",
             "FilteredRowsByLazyRead"]).toString()
    assertTrue(counterSum(multiRowGroupColdProfile, "RowGroupsTotalNum") > 1,
               "The generated Variant file did not contain multiple Parquet row groups")
    assertTrue(counterSum(multiRowGroupColdProfile, "VariantDirectLeafPathMisses") > 0,
               "The unshredded scan did not record its direct-leaf fallback")
    assertTrue(counterSum(multiRowGroupColdProfile, "VariantReconstructedRows") > 0,
               "The unshredded scan did not reconstruct Variant rows")
    assertTrue(counterSum(multiRowGroupColdProfile, "FilteredRowsByLazyRead") > 0,
               "The unshredded Variant predicate did not defer non-predicate columns")
    String multiRowGroupWarmToken =
            "iceberg_variant_multi_row_group_warm_" + UUID.randomUUID().toString()
    sql """
        SELECT '${multiRowGroupWarmToken}', COUNT(*), MIN(id), MAX(id)
        FROM variant_multi_row_group
        WHERE CAST(v['n'] AS INT) >= 8000
    """
    String multiRowGroupWarmProfile = getProfileByToken(multiRowGroupWarmToken,
            ["VariantDirectLeafPathMisses"]).toString()
    assertTrue(counterSum(multiRowGroupWarmProfile, "VariantDirectLeafPathMisses") > 0,
               "The warm unshredded scan did not preserve its direct-leaf fallback")
    qt_variant_multi_row_group_result """
        SELECT COUNT(*), MIN(id), MAX(id), SUM(CAST(v['n'] AS BIGINT))
        FROM variant_multi_row_group
        WHERE CAST(v['n'] AS INT) >= 8000
    """

    qt_variant_deletion_vector_current """
        SELECT COUNT(*), MIN(id), MAX(id), SUM(CAST(v['n'] AS BIGINT))
        FROM variant_deletion_vector
        WHERE v['n'] >= 0
    """
    qt_variant_deletion_vector_before_delete """
        SELECT COUNT(*), MIN(id), MAX(id), SUM(CAST(v['n'] AS BIGINT))
        FROM variant_deletion_vector FOR VERSION AS OF ${deletionVectorBaseSnapshot}
        WHERE v['n'] >= 0
    """
    order_qt_variant_equality_delete_current """
        SELECT id, CAST(v['n'] AS INT), CAST(v['label'] AS STRING), CAST(v AS STRING)
        FROM variant_equality_delete
        WHERE v['n'] >= 0
        ORDER BY id
    """
    order_qt_variant_equality_delete_before_delete """
        SELECT id, CAST(v['n'] AS INT), CAST(v['label'] AS STRING), CAST(v AS STRING)
        FROM variant_equality_delete FOR VERSION AS OF ${equalityDeleteBaseSnapshot}
        WHERE v['n'] >= 0
        ORDER BY id
    """

    // Keep the root Variant as output while the scalar comparison exercises the fallback path for
    // the unshredded Spark files.
    order_qt_variant_implicit_filter """
        SELECT id, CAST(v AS STRING)
        FROM variant_values
        WHERE v['n'] > 35
        ORDER BY id
    """

    qt_variant_shredded_only_time_travel """
        SELECT COUNT(*), MIN(id), MAX(id), SUM(CAST(v['n'] AS BIGINT))
        FROM variant_page_pruning FOR VERSION AS OF ${shreddedOnlySnapshot}
        WHERE CAST(v['n'] AS INT) > 3000
    """
    qt_variant_mixed_before_delete """
        SELECT COUNT(*), MIN(id), MAX(id), SUM(CAST(v['n'] AS BIGINT))
        FROM variant_page_pruning FOR VERSION AS OF ${mixedBeforeDeleteSnapshot}
        WHERE CAST(v['n'] AS INT) > 3000
    """

    // The complete Variant is the only scanned output column outside the predicate. A positive
    // lazy-read count therefore proves Variant output deferral rather than deferral of an id
    // sibling, while the row relationship proves reconstruction happens after filtering.
    String lazyVariantToken =
            "iceberg_variant_lazy_materialization_" + UUID.randomUUID().toString()
    List<List<Object>> lazyVariantRows = sql """
        SELECT '${lazyVariantToken}', CAST(v AS STRING)
        FROM variant_page_pruning FOR VERSION AS OF ${shreddedOnlySnapshot}
        WHERE CAST(v['n'] AS INT) > 3000
    """
    String lazyVariantProfile = getProfileByToken(lazyVariantToken,
            ["VariantDirectLeafRows", "VariantReconstructedRows",
             "FilteredRowsByLazyRead"]).toString()
    long reconstructedVariantRows =
            counterSum(lazyVariantProfile, "VariantReconstructedRows")
    assertEquals((long) lazyVariantRows.size(), reconstructedVariantRows,
            "Complete Variant reconstruction must be limited to selected output rows")
    assertTrue(counterSum(lazyVariantProfile, "VariantDirectLeafRows") >
                    reconstructedVariantRows,
            "Variant output was not deferred until after its shredded-leaf predicate")
    assertTrue(counterSum(lazyVariantProfile, "FilteredRowsByLazyRead") > 0,
            "The shredded predicate did not defer complete Variant output")

    // The query projects the complete Variant while its predicate reads the shredded typed leaf.
    // The appended unshredded file must fall back independently in the same scan.
    String pagePruningToken = "iceberg_variant_page_pruning_" + UUID.randomUUID().toString()
    sql """
        SELECT '${pagePruningToken}', id, CAST(v AS STRING)
        FROM variant_page_pruning
        WHERE CAST(v['n'] AS INT) > 3000
        ORDER BY id
    """
    String pagePruningProfile = getProfileByToken(pagePruningToken,
            ["FilteredRowsByPage", "VariantLeafProjections", "VariantDirectLeafPathMisses",
             "VariantDirectLeafRows", "VariantReconstructedRows"]).toString()
    assertTrue(counterSum(pagePruningProfile, "FilteredRowsByPage") > 0,
               "Shredded Variant typed_value did not filter any Parquet page")
    // The predicate_access_paths contract keeps the typed leaf eager while the complete Variant
    // root is read through the independent deferred-output projection.
    assertTrue(counterSum(pagePruningProfile, "VariantLeafProjections") > 0,
               "A root Variant output query did not retain its typed predicate leaf projection")
    assertTrue(counterSum(pagePruningProfile, "VariantDirectLeafPathMisses") > 0,
               "The mixed scan did not fall back for its unshredded Variant file")
    assertTrue(counterSum(pagePruningProfile, "VariantDirectLeafRows") > 0,
               "The mixed scan did not evaluate rows from the shredded typed leaf")
    assertTrue(counterSum(pagePruningProfile, "VariantReconstructedRows") > 0,
               "The mixed scan did not reconstruct complete Variant output")
    String leafProjectionToken =
            "iceberg_variant_leaf_projection_" + UUID.randomUUID().toString()
    sql """
        SELECT '${leafProjectionToken}', COUNT(*)
        FROM variant_page_pruning
        WHERE CAST(v['n'] AS INT) > 3000
    """
    String leafProjectionProfile = getProfileByToken(leafProjectionToken,
            ["VariantLeafProjections"]).toString()
    assertTrue(counterSum(leafProjectionProfile, "VariantLeafProjections") > 0,
               "Variant typed predicate did not retain a physical leaf projection")
    qt_variant_page_pruning_result """
        SELECT COUNT(*), MIN(id), MAX(id)
        FROM variant_page_pruning
        WHERE CAST(v['n'] AS INT) > 3000
    """

    order_qt_variant_aggregate """
        SELECT CAST(v['ok'] AS BOOLEAN),
               COUNT(*),
               SUM(CAST(v['n'] AS INT)),
               ROUND(AVG(CAST(v['ratio'] AS DOUBLE)), 2)
        FROM variant_values
        WHERE v['name'] IS NOT NULL
        GROUP BY CAST(v['ok'] AS BOOLEAN)
        ORDER BY 1
    """

    order_qt_variant_join """
        WITH thresholds AS (
            SELECT 20 AS n, 'twenty' AS label
            UNION ALL
            SELECT 50 AS n, 'fifty' AS label
        )
        SELECT t.id, d.label, CAST(t.v['name'] AS STRING)
        FROM variant_values t
        JOIN thresholds d ON CAST(t.v['n'] AS INT) = d.n
        ORDER BY t.id
    """

    qt_variant_null_count_distinct """
        SELECT COUNT(*), COUNT(v), SUM(v IS NULL), COUNT(DISTINCT v)
        FROM variant_values
    """

    qt_variant_count_pushdown """
        SELECT COUNT(v), COUNT(*)
        FROM variant_values
    """

    order_qt_variant_canonical_group """
        SELECT CAST(v AS STRING), COUNT(*)
        FROM variant_values
        GROUP BY v
        HAVING COUNT(*) > 1
        ORDER BY 1
    """

    order_qt_variant_nested_projection """
        SELECT id,
               info.label,
               CAST(info.payload AS STRING),
               CAST(events[1] AS STRING),
               CAST(element_at(attrs, 'primary') AS STRING)
        FROM variant_nested
        ORDER BY id
    """

    // Spark may leave nested Variant values unshredded even when top-level shredding is enabled.
    // Keep the external-table regression focused on correctness; mapper/reader unit tests use a
    // physical typed_value fixture to verify nested leaf projection.
    order_qt_variant_nested_filter """
        SELECT id
        FROM variant_nested
        WHERE CAST(info.payload['x'] AS INT) > 0
        ORDER BY id
    """

    // Signed integer selectors are array indexes, even when a shredded object has a key with the
    // same serialized token. The ambiguous scanner path must retain enough state for both results.
    List<List<Object>> signedSelectorRows = sql """
        SELECT CAST(v[-1] AS INT), CAST(v['-1'] AS INT)
        FROM variant_signed_selector
    """
    assertEquals(1, signedSelectorRows.size())
    assertEquals(null, signedSelectorRows[0][0])
    assertEquals("41", signedSelectorRows[0][1].toString())

    order_qt_variant_nested_expressions """
        SELECT id,
               CAST(info.payload['x'] AS INT),
               CAST(info.payload['deep']['name'] AS STRING),
               CAST(events[1]['kind'] AS STRING),
               CAST(events[2]['score'] AS INT),
               CAST(element_at(attrs, 'primary')['enabled'] AS BOOLEAN),
               CAST(element_at(attrs, 'primary')['score'] AS INT) + 1
        FROM variant_nested
        ORDER BY id
    """

    order_qt_variant_evolution_renamed_snapshot """
        SELECT id, CAST(event['stage'] AS STRING), CAST(event['metric'] AS INT), note
        FROM variant_evolution FOR VERSION AS OF ${evolutionRenamed}
        ORDER BY id
    """

    order_qt_variant_evolution_renamed_tag """
        SELECT id, CAST(event['stage'] AS STRING), CAST(event['metric'] AS INT), note
        FROM variant_evolution FOR VERSION AS OF 'variant_renamed'
        ORDER BY id
    """

    order_qt_variant_evolution_added_reordered """
        SELECT id,
               CAST(event['stage'] AS STRING),
               CAST(aux['side'] AS INT),
               note
        FROM variant_evolution FOR VERSION AS OF ${evolutionWithAux}
        ORDER BY id
    """

    order_qt_variant_evolution_dropped """
        SELECT id, CAST(sidecar['side'] AS INT), note
        FROM variant_evolution FOR VERSION AS OF ${evolutionDropped}
        ORDER BY id
    """

    test {
        sql """
            SELECT event
            FROM variant_evolution FOR VERSION AS OF ${evolutionDropped}
        """
        exception "event"
    }

    order_qt_variant_evolution_drop_readd """
        SELECT id,
               CAST(sidecar['side'] AS INT),
               CAST(event['stage'] AS STRING),
               CAST(event['metric'] AS INT),
               note
        FROM variant_evolution FOR VERSION AS OF ${evolutionReadded}
        ORDER BY id
    """

    test {
        sql """SELECT payload FROM variant_evolution"""
        exception "payload"
    }

    sql """
        INSERT INTO variant_write_guard (id)
        SELECT id
        FROM variant_write_guard FOR VERSION AS OF ${writeGuardSourceSnapshot}
    """
    List<List<Object>> sparkPartialVariantRows = spark_iceberg """
        SELECT COUNT(*), COUNT(payload)
        FROM demo.${dbName}.variant_write_guard
    """
    assertEquals("2", sparkPartialVariantRows[0][0].toString())
    assertEquals("0", sparkPartialVariantRows[0][1].toString())

    // A delete-only MERGE still emits only position deletes and does not create a data file.
    String beforePositionDeleteSnapshot = latestSnapshotId("variant_values")
    sql """
        MERGE INTO variant_values t
        USING (SELECT 11 AS id) s
        ON t.id = s.id
        WHEN MATCHED THEN DELETE
    """
    qt_variant_delete_only_merge "SELECT COUNT(*) FROM variant_values WHERE id = 11"
    order_qt_variant_position_delete_alignment """
        SELECT id, CAST(v['name'] AS STRING), CAST(v['n'] AS INT), CAST(v AS STRING)
        FROM variant_values
        WHERE v['n'] >= 40
        ORDER BY id
    """
    order_qt_variant_before_position_delete """
        SELECT id, CAST(v['name'] AS STRING), CAST(v['n'] AS INT), CAST(v AS STRING)
        FROM variant_values FOR VERSION AS OF ${beforePositionDeleteSnapshot}
        WHERE v['n'] >= 40
        ORDER BY id
    """
    String positionDeleteToken =
            "iceberg_variant_position_delete_" + UUID.randomUUID().toString()
    sql """
        SELECT '${positionDeleteToken}', COUNT(*)
        FROM variant_values
        WHERE v['n'] >= 40
    """
    String positionDeleteProfile = getProfileByToken(positionDeleteToken,
            ["VariantDirectLeafPathMisses", "VariantReconstructedRows"]).toString()
    assertTrue(counterSum(positionDeleteProfile, "VariantDirectLeafPathMisses") > 0,
               "Position-delete filtering did not preserve the unshredded Variant fallback")
    assertTrue(counterSum(positionDeleteProfile, "VariantReconstructedRows") > 0,
               "Position-delete filtering did not reconstruct its Variant rows")

    // Files written before the Variant field existed have no physical Variant payload. Schema
    // evolution must synthesize NULL instead of rejecting their non-Parquet file format.
    // Metadata-only MODIFY is valid on an existing Iceberg v3 ORC Variant table. Doris still
    // rejects data writes because Iceberg ORC has no Variant writer.
    sql """ALTER TABLE variant_orc MODIFY COLUMN v VARIANT COMMENT 'orc payload'"""
    order_qt_variant_orc_missing_column """
        SELECT id, CAST(v AS STRING) FROM variant_orc ORDER BY id
    """
    test {
        sql """INSERT INTO variant_orc VALUES (2, CAST('{"format":"orc"}' AS VARIANT))"""
        exception "Iceberg VARIANT writes require Parquet data files"
    }
    qt_variant_orc_count_star "SELECT COUNT(*) FROM variant_orc"
    order_qt_variant_mixed_format """
        SELECT id, CAST(v AS STRING) FROM variant_mixed_format ORDER BY id
    """

    sql """set enable_file_scanner_v2=false"""
    try {
        test {
            sql """SELECT CAST(v AS STRING) FROM variant_values ORDER BY id"""
            exception "legacy file scanner does not support VARIANT"
        }
    } finally {
        sql """set enable_file_scanner_v2=true"""
    }
}
