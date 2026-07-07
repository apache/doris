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

suite("test_iceberg_deletion_vector", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_deletion_vector"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""


    sql """switch ${catalog_name};"""
    sql """ use format_v3;"""
    sql """ set file_split_size=1;"""

    try {
    def executeCommandWithStatus = { String cmd, int timeoutSeconds = 300, Boolean logFailure = true,
            Boolean logCommand = true ->
        StringBuilder stdout = new StringBuilder()
        StringBuilder stderr = new StringBuilder()
        try {
            if (logCommand) {
                logger.info("execute ${cmd}")
            }
            def proc = new ProcessBuilder("/bin/bash", "-c", cmd).start()
            proc.consumeProcessOutput(stdout, stderr)
            proc.waitForOrKill(timeoutSeconds * 1000)
            int exitcode = proc.exitValue()
            String output = stdout.toString()
            String error = stderr.toString()
            if (exitcode != 0 && logFailure) {
                logger.info("exit code: ${exitcode}, stdout\n: ${output}\nstderr\n: ${error}")
            }
            return [exitCode: exitcode, stdout: output, stderr: error]
        } catch (IOException e) {
            assertTrue(false, "Execute failed: ${cmd}, err: ${e.message}")
        }
    }

    def executeCommand = { String cmd, Boolean mustSuc, int timeoutSeconds = 300 ->
        def result = executeCommandWithStatus(cmd, timeoutSeconds)
        if (mustSuc && result.exitCode != 0) {
            assertTrue(false,
                    "Execute failed: ${cmd}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}")
        }
        return result.stdout
    }

    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    def listDockerContainers = {
        String containers =
                executeCommand("${dockerCommand} ps --format '{{.ID}}\t{{.Names}}\t{{.Image}}'", true, 30) ?:
                        ""
        return containers.readLines().collect { it.trim() }.findAll { !it.isEmpty() }
    }

    def findRequiredDockerContainer = { String role, String configKey, String probeCommand ->
        String configuredContainer = context.config.otherConfigs.get(configKey)
        if (configuredContainer != null && !configuredContainer.isEmpty()) {
            def probe = executeCommandWithStatus(
                    "${dockerCommand} exec ${configuredContainer} bash -lc '${probeCommand}'",
                    30)
            assertEquals(0, probe.exitCode,
                    "${role} container configured by ${configKey}=${configuredContainer} is not usable")
            return configuredContainer
        }

        def matchedContainers = []
        listDockerContainers().each { String containerLine ->
            def fields = containerLine.split(/\t/, 3)
            assertTrue(fields.length >= 2, "Unexpected docker ps output: ${containerLine}")
            String containerId = fields[0].trim()
            String containerName = fields[1].trim()
            String containerImage = fields.length >= 3 ? fields[2].trim() : ""
            def probe = executeCommandWithStatus(
                    "${dockerCommand} exec ${containerId} bash -lc '${probeCommand}'",
                    30,
                    false,
                    false)
            if (probe.exitCode == 0) {
                matchedContainers.add(
                        [id: containerId, name: containerName, image: containerImage])
            }
        }

        assertFalse(matchedContainers.isEmpty(),
                "No usable ${role} container found. Set ${configKey} to the exact container name " +
                        "or start the required external environment.")
        assertEquals(1, matchedContainers.size(),
                "Multiple usable ${role} containers found: ${matchedContainers}. " +
                        "Set ${configKey} to the exact container name.")
        logger.info("use ${role} container ${matchedContainers[0].name} " +
                "(${matchedContainers[0].image})")
        return matchedContainers[0].id
    }

    String sparkContainerName = findRequiredDockerContainer(
            "Spark Iceberg", "icebergSparkContainer",
            "command -v spark-sql >/dev/null && test -f /mnt/SUCCESS && " +
                    "spark-sql -e \"SHOW NAMESPACES IN demo\" >/dev/null")

    def encodeBase64 = { String text ->
        return text.getBytes("UTF-8").encodeBase64().toString()
    }

    def runInSparkContainer = { String command, int timeout = 300 ->
        executeCommand("${dockerCommand} exec ${sparkContainerName} bash -lc '${command}'", true,
                timeout)
    }

    def runSparkSql = { String sqlText, int timeout = 600 ->
        String encodedSql = encodeBase64(sqlText)
        runInSparkContainer(
                "echo ${encodedSql} | base64 -d >/tmp/test_iceberg_deletion_vector.sql && " +
                        "spark-sql -f /tmp/test_iceberg_deletion_vector.sql",
                timeout
        )
    }

    String setupSql = """
use format_v3;

-- Delete-type matrix fixtures: reuse existing v2 equality/position-delete tables and create
-- v3 MOR tables that Spark writes with Iceberg deletion vectors.
drop table if exists dv_delete_matrix_equality_only;
CALL demo.system.register_table(
    table => 'format_v3.dv_delete_matrix_equality_only',
    metadata_file => 's3a://warehouse/wh/test_db/customer_flink_one/metadata/00002-c38103a7-237b-4350-aa98-05fe2ad14d16.metadata.json'
);

drop table if exists dv_delete_matrix_position_only;
CALL demo.system.register_table(
    table => 'format_v3.dv_delete_matrix_position_only',
    metadata_file => 's3a://warehouse/wh/test_db/iceberg_position_parquet/metadata/00005-6ff36319-7a9a-4357-a9fc-965d5f280c9b.metadata.json'
);

drop table if exists dv_delete_matrix_position_and_dv;
CALL demo.system.register_table(
    table => 'format_v3.dv_delete_matrix_position_and_dv',
    metadata_file => 's3a://warehouse/wh/test_db/iceberg_position_parquet/metadata/00005-6ff36319-7a9a-4357-a9fc-965d5f280c9b.metadata.json'
);
alter table dv_delete_matrix_position_and_dv set tblproperties (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
delete from dv_delete_matrix_position_and_dv where id = 2;

drop table if exists dv_delete_matrix_equality_and_dv;
create table dv_delete_matrix_equality_and_dv (
    id int,
    batch int,
    data string
)
using iceberg
tblproperties (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
insert into dv_delete_matrix_equality_and_dv values
    (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd'),
    (5, 1, 'e'), (6, 1, 'f'), (7, 1, 'g'), (8, 1, 'h'),
    (9, 2, 'i'), (10, 2, 'j'), (11, 2, 'k'), (12, 2, 'l');
delete from dv_delete_matrix_equality_and_dv where id % 2 = 1;

-- Split/cache fixture: one sufficiently large data file plus one DV. Doris reads it with a tiny
-- file_split_size so the same data file is split across scanners and the shared scan-node cache
-- must prevent the DV from being parsed once per split.
drop table if exists dv_split_cache_single_file;
create table dv_split_cache_single_file (
    id bigint,
    grp int,
    payload string
)
using iceberg
tblproperties (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.parquet.row-group-size-bytes' = '1024'
);
set spark.sql.shuffle.partitions = 1;
insert into dv_split_cache_single_file
select /*+ COALESCE(1) */
       id,
       cast(id % 8 as int) as grp,
       repeat(concat('payload_', cast(id as string)), 20) as payload
  from range(0, 4096);
delete from dv_split_cache_single_file where id % 2 = 1;
reset spark.sql.shuffle.partitions;
"""
    runSparkSql(setupSql)

    String javaSource = '''
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class AppendEqualityDelete {
    public static void main(String[] args) throws Exception {
        Catalog catalog = IcebergRestCatalog.load();
        Table table = catalog.loadTable(TableIdentifier.of(args[0], args[1]));
        Schema eqSchema = table.schema().select(args[2]);
        int fieldId = table.schema().findField(args[2]).fieldId();
        String location = table.location() + "/data/equality-delete-" + args[2] + "-"
                + args[3] + "-" + System.currentTimeMillis() + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(location);
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(outputFile)
                .forTable(table)
                .rowSchema(eqSchema)
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(fieldId)
                .overwrite()
                .buildEqualityWriter();
        GenericRecord record = GenericRecord.create(eqSchema);
        record.setField(args[2], Integer.valueOf(args[3]));
        writer.write(record);
        writer.close();
        DeleteFile deleteFile = writer.toDeleteFile();
        table.newRowDelta().addDeletes(deleteFile).commit();
    }
}

class ReadIcebergRows {
    public static void main(String[] args) throws Exception {
        Catalog catalog = IcebergRestCatalog.load();
        Table table = catalog.loadTable(TableIdentifier.of(args[0], args[1]));
        String[] columns = Arrays.copyOfRange(args, 2, args.length);
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).select(columns).build()) {
            for (Record record : records) {
                List<String> values = new ArrayList<>();
                for (String column : columns) {
                    values.add(String.valueOf(record.getField(column)));
                }
                rows.add(String.join("\\t", values));
            }
        }
        Collections.sort(rows, Comparator.comparingInt(row -> Integer.parseInt(row.split("\\t")[0])));
        for (String row : rows) {
            System.out.println(row);
        }
    }
}

class IcebergRestCatalog {
    static Catalog load() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "rest");
        props.put("uri", "http://rest:8181");
        props.put("warehouse", "s3://warehouse/wh/");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", "http://minio:9000");
        props.put("s3.path-style-access", "true");
        props.put("s3.region", "us-east-1");
        return CatalogUtil.buildIcebergCatalog("demo", props, null);
    }
}
'''
    String encodedJavaSource = encodeBase64(javaSource)
    runInSparkContainer(
            "echo ${encodedJavaSource} | base64 -d >/tmp/AppendEqualityDelete.java && " +
                    "javac -cp \"/opt/spark/jars/*\" /tmp/AppendEqualityDelete.java && " +
                    "java -cp \"/tmp:/opt/spark/jars/*\" AppendEqualityDelete " +
                    "format_v3 dv_delete_matrix_equality_and_dv id 10",
            300
    )


    // Baseline DV correctness across Parquet, ORC, no-delete, and v2-to-v3 upgrade fixtures.
    qt_q1 """ SELECT * FROM dv_test ORDER BY id; """
    qt_q2 """ SELECT * FROM dv_test_v2 ORDER BY id; """
    qt_q3 """ SELECT * FROM dv_test_orc ORDER BY id; """
    qt_no_delete """ SELECT * FROM dv_test_no_delete ORDER BY id; """

    // Delete-type matrix checks cover equality-only, position-only, DV-only, DV+position,
    // and DV+equality combinations.
    qt_equality_only """ SELECT * FROM dv_delete_matrix_equality_only ORDER BY id; """
    qt_position_only_count """ SELECT count(*) FROM dv_delete_matrix_position_only; """
    qt_position_and_dv_count """ SELECT count(*) FROM dv_delete_matrix_position_and_dv; """
    qt_equality_and_dv """ SELECT * FROM dv_delete_matrix_equality_and_dv ORDER BY id; """
    qt_split_cache_single_file_count """
        SELECT count(*), sum(id)
          FROM dv_split_cache_single_file;
    """

    qt_q4 """ SELECT * FROM dv_test where id = 2 ORDER BY id ; """
    qt_q5 """ SELECT * FROM dv_test_v2 where id = 2 ORDER BY id  ; """
    qt_q5_orc """ SELECT * FROM dv_test_orc where id = 2 ORDER BY id; """

    qt_q6 """ SELECT * FROM dv_test  where id %2 =1 ORDER BY id; """
    qt_q7 """ SELECT * FROM dv_test_v2  where id %2 =1 ORDER BY id; """
    qt_q7_orc """ SELECT * FROM dv_test_orc where id %2 =1 ORDER BY id; """

    qt_q8 """ SELECT * FROM dv_test where data < 'f' ORDER BY id; """
    qt_q9 """ SELECT * FROM dv_test_v2 where data < 'f' ORDER BY id; """
    qt_q9_orc """ SELECT * FROM dv_test_orc where data < 'f' ORDER BY id; """


    qt_q10 """ SELECT count(*) FROM dv_test ; """
    qt_q11 """ SELECT count(*) FROM dv_test_v2 ; """
    qt_q11_orc """ SELECT count(*) FROM dv_test_orc; """
    qt_no_delete_count """ SELECT count(*) FROM dv_test_no_delete; """


    qt_q12 """ SELECT count(id) FROM dv_test ; """
    qt_q13 """ SELECT count(id) FROM dv_test_v2 ; """
    qt_q13_orc """ SELECT count(id) FROM dv_test_orc; """
    
    qt_q14 """ SELECT count(batch) FROM dv_test ; """
    qt_q15 """ SELECT count(batch) FROM dv_test_v2 ; """
    qt_q15_orc """ SELECT count(batch) FROM dv_test_orc; """


    qt_q16 """ SELECT count(*) FROM dv_test_1w ; """
    qt_q17 """ SELECT count(id) FROM dv_test_1w ; """
    qt_q18 """ SELECT count(grp) FROM dv_test_1w where id = 1; """
    qt_q19 """ SELECT count(value) FROM dv_test_1w where id%2 = 1; """
    qt_q20 """ SELECT count(id) FROM dv_test_1w where id%3 = 1; """
    qt_q21 """ SELECT count(ts) FROM dv_test_1w where id%3 != 1; """
    qt_q22 """ SELECT * FROM dv_test ORDER BY data DESC LIMIT 3; """
    qt_q23 """ SELECT * FROM dv_test_v2 ORDER BY data DESC LIMIT 3; """
    qt_q24 """ SELECT batch, count(*), sum(id) FROM dv_test GROUP BY batch ORDER BY batch; """
    qt_q25 """ SELECT batch, count(*), sum(id) FROM dv_test_v2 GROUP BY batch ORDER BY batch; """
    qt_q26 """ SELECT batch, count(*), sum(id) FROM dv_test_orc GROUP BY batch ORDER BY batch; """
    qt_q27 """ SELECT id, data FROM dv_test WHERE id BETWEEN 2 AND 10 ORDER BY id LIMIT 2; """
    qt_q28 """ SELECT id, data FROM dv_test_v2 WHERE id BETWEEN 2 AND 10 ORDER BY id LIMIT 2; """
    qt_q29 """ SELECT id, data FROM dv_test_orc WHERE id BETWEEN 2 AND 10 ORDER BY id LIMIT 2; """
    qt_q30 """ SELECT count(*) FROM (SELECT * FROM dv_test_1w WHERE id % 7 = 0 ORDER BY id LIMIT 100) t; """
    qt_q31 """ SELECT count(*) FROM (SELECT * FROM dv_test_1w WHERE id % 11 = 0 ORDER BY id LIMIT 100) t; """

    // Metadata matrix verifies the physical delete-file content types that back the logical
    // scenario checks above.
    qt_delete_type_matrix """
        SELECT 'dv_equality' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_delete_matrix_equality_and_dv\$files`
         GROUP BY content, file_format
        UNION ALL
        SELECT 'dv_only' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_test\$files`
         GROUP BY content, file_format
        UNION ALL
        SELECT 'dv_position' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_delete_matrix_position_and_dv\$files`
         GROUP BY content, file_format
        UNION ALL
        SELECT 'equality_only' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_delete_matrix_equality_only\$files`
         GROUP BY content, file_format
        UNION ALL
        SELECT 'no_delete' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_test_no_delete\$files`
         GROUP BY content, file_format
        UNION ALL
        SELECT 'position_only' scenario, content, file_format, count(*) files, sum(record_count) records
          FROM `dv_delete_matrix_position_only\$files`
         GROUP BY content, file_format
         ORDER BY scenario, content, file_format;
    """

    // Iceberg v3 semantics: when a data file has a DV, old position deletes for that file must be
    // ignored, while equality deletes still filter matching rows.
    qt_position_delete_ignored_with_dv """
        SELECT id, name
          FROM dv_delete_matrix_position_and_dv
         WHERE id IN (1, 2, 5, 10, 15)
         ORDER BY id, name
         LIMIT 20;
    """
    qt_equality_delete_active_with_dv """
        SELECT id, batch, data
          FROM dv_delete_matrix_equality_and_dv
         ORDER BY id;
    """

    def normalizeExternalRows = { String output ->
        return output.readLines()
                .collect { it.trim() }
                .findAll { it ==~ /^-?[0-9].*/ && it.contains("\t") }
                .join("\n")
    }

    String expectedRows = ["2\t1\tb", "4\t1\td", "6\t1\tf", "8\t1\th", "12\t2\tl"].join("\n")

    String sparkRows = normalizeExternalRows(runSparkSql(
            "use format_v3; select id, batch, data from dv_delete_matrix_equality_and_dv order by id;",
            300
    ))
    assertEquals(expectedRows, sparkRows)

    String javaRows = normalizeExternalRows(runInSparkContainer(
            "java -cp \"/tmp:/opt/spark/jars/*\" ReadIcebergRows " +
                    "format_v3 dv_delete_matrix_equality_and_dv id batch data",
            300
    ))
    assertEquals(expectedRows, javaRows)

    String trinoContainerName = findRequiredDockerContainer(
            "Trino", "icebergTrinoContainer",
            "test -d /etc/trino/catalog && command -v trino >/dev/null")
    String trinoExternalEnvIp = externalEnvIp
    if (trinoExternalEnvIp == "127.0.0.1" || trinoExternalEnvIp.equalsIgnoreCase("localhost")) {
        trinoExternalEnvIp = executeCommand("hostname -I | cut -d' ' -f1", true, 30)?.trim()
    }
    String trinoCatalogProps = """
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://${trinoExternalEnvIp}:${rest_port}
fs.native-s3.enabled=true
s3.endpoint=http://${trinoExternalEnvIp}:${minio_port}
s3.aws-access-key=admin
s3.aws-secret-key=password
s3.region=us-east-1
s3.path-style-access=true
"""
    String encodedTrinoCatalogProps = encodeBase64(trinoCatalogProps)
    executeCommand(
            "${dockerCommand} exec ${trinoContainerName} bash -lc " +
                    "'echo ${encodedTrinoCatalogProps} | " +
                    "base64 -d >/etc/trino/catalog/iceberg.properties'",
            true,
            30
    )
    executeCommand("${dockerCommand} restart ${trinoContainerName}", true, 60)
    String trinoRows = ""
    for (int i = 0; i < 12; i++) {
        Thread.sleep(5000)
        trinoRows = normalizeExternalRows(executeCommand(
                "${dockerCommand} exec ${trinoContainerName} trino --output-format TSV " +
                        "--catalog iceberg --schema format_v3 --execute " +
                        "\"SELECT id, batch, data " +
                        "FROM dv_delete_matrix_equality_and_dv ORDER BY id\"",
                false,
                120
        ))
        if (!trinoRows.isEmpty()) {
            break
            }
    }
    assertEquals(expectedRows, trinoRows)

    def profileCounterMax = { String profileText, String counterName ->
        long maxValue = Long.MIN_VALUE
        profileText.readLines().findAll { it.contains(counterName + ":") }.each { line ->
            def matcher = line =~ /\(([0-9,]+)\)/
            String rawValue
            if (matcher.find()) {
                rawValue = matcher.group(1)
            } else {
                matcher = line =~ /:\s*([0-9,]+)/
                rawValue = matcher.find() ? matcher.group(1) : null
            }
            if (rawValue != null) {
                maxValue = Math.max(maxValue, Long.parseLong(rawValue.replace(",", "")))
            }
        }
        return maxValue == Long.MIN_VALUE ? 0L : maxValue
    }

    def profileInfoValueCount = { String profileText, String infoName ->
        def matcher = profileText =~ (java.util.regex.Pattern.quote(infoName) + ":\\s*\\[([^\\]]*)\\]")
        if (!matcher.find()) {
            return 0
        }
        return matcher.group(1).split(",").collect { it.trim() }.findAll { !it.isEmpty() }.size()
    }

    // Split/cache scenario for 3.4: this query scans one data file with file_split_size=1. The
    // result is captured by qt_split_cache_single_file_count; the profile assertion checks the DV
    // rows were materialized once for the shared scan-node cache, not once per split.
    String splitCacheProfileTag = "iceberg_dv_split_cache_" + UUID.randomUUID().toString()
    profile(splitCacheProfileTag) {
        run {
            sql """
                /* ${splitCacheProfileTag} */
                SELECT count(*), sum(id)
                  FROM dv_split_cache_single_file;
            """
            // The detailed scanner counters are populated asynchronously after the query returns.
            Thread.sleep(5000)
        }
        check { profileString, exception ->
            if (exception != null) {
                throw exception
            }
            long numDeleteRows = profileCounterMax(profileString, "NumDeleteRows")
            int scannerCount = profileInfoValueCount(profileString, "PerScannerRowsRead")
            assertEquals(2048L, numDeleteRows)
            assertTrue(scannerCount > 1,
                    "Expected multiple scanner entries for split DV scan, profile: ${profileString}")
        }
    }

    // Cross-reader comparison protects against Doris accepting a DV/equality-delete result that
    // diverges from Spark, Trino, or Iceberg's Java reader.
    explain {
        sql("SELECT count(*) FROM dv_test;")
        verbose(true)
        contains "deleteFileNum"
    }

    explain {
        sql("SELECT count(*) FROM dv_test_orc;")
        verbose(true)
        contains "deleteFileNum"
    }

    explain {
        sql("SELECT count(*) FROM dv_test_v2;")
        verbose(true)
        contains "deleteFileNum"
    }

    } finally {
        sql """ unset variable file_split_size;"""
    }
}
