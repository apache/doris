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
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }
    sql "SET ENABLE_VARIANT_V2=true"

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
            'write.parquet.shred-variants'='true',
            'write.parquet.variant-inference-buffer-size'='100'
        );
        INSERT INTO demo.${dbName}.variant_values
        WITH (`shred-variants`=true, `variant-inference-buffer-size`=100) VALUES
            (8, parse_json('{"ok":true,"n":30,"name":"same"}')),
            (9, parse_json('{"name":"carol","n":40,"ratio":4.5,"ok":true,"arr":[5,6],"nested":{"city":"bj"},"new_key":"new"}')),
            (10, parse_json('{"name":"dave","n":50,"ratio":5.5,"ok":false,"arr":[7,8],"nested":{"city":"sz"}}')),
            (11, parse_json('{"name":null,"n":60,"ratio":6.5,"ok":true,"arr":[9,10],"nested":{"city":null}}'));

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
            'write.parquet.shred-variants'='true',
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

        DROP TABLE IF EXISTS demo.${dbName}.variant_signed_selector;
        CREATE TABLE demo.${dbName}.variant_signed_selector (
            id INT,
            v VARIANT
        ) USING iceberg
        TBLPROPERTIES (
            'format-version'='3',
            'write.format.default'='parquet',
            'write.parquet.shred-variants'='true',
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
    """

    String writeGuardSourceSnapshot = latestSnapshotId("variant_write_guard")
    spark_iceberg """
        ALTER TABLE demo.${dbName}.variant_write_guard ADD COLUMN payload VARIANT
    """

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

    def profileAction = new ProfileAction(context)
    def getProfileByToken = { String token ->
        for (int retry = 0; retry < 20; ++retry) {
            List profileData = profileAction.getProfileList()
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    return profileAction.getProfile(profileItem["Profile ID"].toString())
                }
            }
            Thread.sleep(500)
        }
        throw new IllegalStateException("Missing profile for token: " + token)
    }
    def counterSum = { String profile, String counterName ->
        Pattern pattern = Pattern.compile(Pattern.quote(counterName) + ":\\s*([0-9,]+)")
        Matcher matcher = pattern.matcher(profile)
        long sum = 0
        while (matcher.find()) {
            sum += Long.parseLong(matcher.group(1).replace(",", ""))
        }
        return sum
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

    order_qt_variant_filter """
        SELECT id, CAST(v['name'] AS STRING), CAST(v['n'] AS INT)
        FROM variant_values
        WHERE CAST(v['n'] AS INT) >= 20
          AND CAST(v['ok'] AS BOOLEAN) = true
        ORDER BY id
    """

    // The first INSERT is unshredded while the second is shredded. Keep both small files on one
    // scanner so their complete and leaf-only physical states must be projected before batching.
    sql "set parallel_pipeline_task_num=1"
    sql "set max_file_scanners_concurrency=1"
    order_qt_variant_cross_file_leaf_projection """
        SELECT id, CAST(v['n'] AS INT)
        FROM variant_values
        ORDER BY id
    """

    // Keep the root Variant as output while the implicit scalar comparison drives the shredded
    // typed_value statistics/page-index path.
    order_qt_variant_implicit_shredded_filter """
        SELECT id, CAST(v AS STRING)
        FROM variant_values
        WHERE v['n'] > 35
        ORDER BY id
    """

    // The query projects the root Variant, while the predicate uses typed_value page metadata.
    String pagePruningToken = "iceberg_variant_page_pruning_" + UUID.randomUUID().toString()
    sql """
        SELECT '${pagePruningToken}', id, CAST(v AS STRING)
        FROM variant_page_pruning
        WHERE v['n'] > 3000
        ORDER BY id
    """
    String pagePruningProfile = getProfileByToken(pagePruningToken).toString()
    assertTrue(counterSum(pagePruningProfile, "FilteredRowsByPage") > 0,
               "Shredded Variant typed_value did not filter any Parquet page")
    // The predicate_access_paths contract keeps the typed leaf eager while the complete Variant
    // root is read through the independent deferred-output projection.
    assertTrue(counterSum(pagePruningProfile, "VariantLeafProjections") > 0,
               "A root Variant output query did not retain its typed predicate leaf projection")
    String leafProjectionToken =
            "iceberg_variant_leaf_projection_" + UUID.randomUUID().toString()
    sql """
        SELECT '${leafProjectionToken}', COUNT(*)
        FROM variant_page_pruning
        WHERE v['n'] > 3000
    """
    String leafProjectionProfile = getProfileByToken(leafProjectionToken).toString()
    assertTrue(counterSum(leafProjectionProfile, "VariantLeafProjections") > 0,
               "Variant typed predicate did not retain a physical leaf projection")
    qt_variant_page_pruning_result """
        SELECT COUNT(*), MIN(id), MAX(id)
        FROM variant_page_pruning
        WHERE v['n'] > 3000
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

    test {
        sql """
            INSERT INTO variant_write_guard (id)
            SELECT id
            FROM variant_write_guard FOR VERSION AS OF ${writeGuardSourceSnapshot}
        """
        exception "Iceberg VARIANT columns are read-only and cannot be written"
    }

    // A delete-only MERGE emits only position deletes. It must remain available even though
    // update/insert actions would route the unchanged Variant through the unsupported data writer.
    sql """
        MERGE INTO variant_values t
        USING (SELECT 11 AS id) s
        ON t.id = s.id
        WHEN MATCHED THEN DELETE
    """
    qt_variant_delete_only_merge "SELECT COUNT(*) FROM variant_values WHERE id = 11"

    // Files written before the Variant field existed have no physical Variant payload. Schema
    // evolution must synthesize NULL instead of rejecting their non-Parquet file format.
    order_qt_variant_orc_missing_column """
        SELECT id, CAST(v AS STRING) FROM variant_orc ORDER BY id
    """
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
