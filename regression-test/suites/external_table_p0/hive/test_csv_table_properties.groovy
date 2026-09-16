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

suite("test_csv_table_properties", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    def checkV2 = { Closure check ->
        def originalScannerV2 = sql("SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]
        try {
            sql "SET enable_file_scanner_v2 = true"
            check()
        } finally {
            sql "SET enable_file_scanner_v2 = ${originalScannerV2}"
        }
    }
    setHivePrefix("hive3")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    hive_docker "CREATE DATABASE IF NOT EXISTS csv_table_properties_db"
    hive_docker "DROP TABLE IF EXISTS csv_table_properties_db.source_rows"
    hive_docker """
        CREATE TABLE csv_table_properties_db.source_rows (
            row_id STRING, label STRING, payload STRING, group_id INT
        ) STORED AS PARQUET
    """
    hive_docker """
        INSERT INTO csv_table_properties_db.source_rows VALUES
            ('1', 'Alpha', 'quiet sequoias, escape q and e', 1),
            ('2', 'Omega', 'edge | requests, end', 4),
            ('3', 'Beta', '"literal quotes", ss qq ee', 4),
            ('4', 'Empty', '', 1)
    """
    hive_docker "SET hive.exec.dynamic.partition=true"
    hive_docker "SET hive.exec.dynamic.partition.mode=nonstrict"

    sql "DROP CATALOG IF EXISTS csv_table_properties_catalog"
    sql """
        CREATE CATALOG csv_table_properties_catalog PROPERTIES (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
        )
    """
    sql "USE csv_table_properties_catalog.csv_table_properties_db"

    // These are the four physical layouts used by CSV CTAS/INSERT product tests. Partition
    // values live outside the file, so a delimiter bug can corrupt data columns while they stay correct.
    def layouts = [
        [table: "csv_three_columns", columns: "row_id STRING, label STRING, payload STRING",
            projection: "row_id, label, payload", aggregate: "max(label), max(payload)", partitioned: false],
        [table: "csv_partitioned_ids", columns: "row_id STRING, label STRING",
            projection: "row_id, label, group_id", aggregate: "max(label), max(group_id)", partitioned: true],
        [table: "csv_two_columns", columns: "label STRING, payload STRING",
            projection: "label, payload", aggregate: "max(label), max(payload)", partitioned: false],
        [table: "csv_partitioned_payload", columns: "label STRING, payload STRING",
            projection: "label, payload, group_id", aggregate: "max(label), max(payload), max(group_id)",
            partitioned: true]
    ]
    for (def layout : layouts) {
        hive_docker "DROP TABLE IF EXISTS csv_table_properties_db.${layout.table}"
        // Keep character settings ONLY in TBLPROPERTIES: SERDEPROPERTIES would hide the lookup bug.
        hive_docker """
            CREATE TABLE csv_table_properties_db.${layout.table} (${layout.columns})
            ${layout.partitioned ? 'PARTITIONED BY (group_id INT)' : ''}
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            STORED AS TEXTFILE
            TBLPROPERTIES ('separatorChar'='s', 'quoteChar'='q', 'escapeChar'='e')
        """
        hive_docker """
            INSERT INTO csv_table_properties_db.${layout.table}
            ${layout.partitioned ? 'PARTITION (group_id)' : ''}
            SELECT ${layout.projection} FROM csv_table_properties_db.source_rows
        """

        // Hive's text writer honors line.delim even though its CSV reader ignores it. Set this only
        // after INSERT so the fixture keeps newline records and tests a read-side metadata override.
        hive_docker """
            ALTER TABLE csv_table_properties_db.${layout.table} SET TBLPROPERTIES ('line.delim'='|')
        """
        def sourceRows = hive_docker """
            SELECT ${layout.projection} FROM csv_table_properties_db.source_rows ORDER BY label
        """
        def hiveRows = hive_docker """
            SELECT ${layout.projection} FROM csv_table_properties_db.${layout.table} ORDER BY label
        """
        assertEquals(sourceRows, hiveRows, "Hive CSV fixture must preserve the source rows: ${layout.table}")

        sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"

        // Compare against the source data in Hive, not another CSV read with the same delimiter bug.
        def queries = [
            "SELECT ${layout.projection} FROM %s ORDER BY label",
            "SELECT ${layout.aggregate} FROM %s",
            "SELECT ${layout.projection} FROM %s WHERE label = 'Omega' ORDER BY label"
        ]
        // The same files must remain readable after a metadata-only change to multi-character values:
        // OpenCSVSerde takes the first Java character of each property.
        for (boolean multiCharacter : [false, true]) {
            if (multiCharacter) {
                hive_docker """
                    ALTER TABLE csv_table_properties_db.${layout.table} SET TBLPROPERTIES (
                        'separatorChar'='ss', 'quoteChar'='qq', 'escapeChar'='ee'
                    )
                """
                sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
            }
            for (String query : queries) {
                def expected = hive_docker(String.format(query, "csv_table_properties_db.source_rows"))
                checkV2 {
                    def actual = sql(String.format(query, layout.table))
                    assertEquals(expected, actual)
                }
            }
        }
    }

    // Hive accepts these characters, but Doris must reject them before truncating their UTF-8 bytes.
    // Empty values are covered by unit tests: Hive itself rejects them while validating ALTER TABLE.
    for (def invalid : [
        [key: "quoteChar", value: "é", message: "the first character must be ASCII"],
        [key: "escapeChar", value: "é", message: "the first character must be ASCII"]
    ]) {
        hive_docker """
            ALTER TABLE csv_table_properties_db.csv_two_columns
            SET TBLPROPERTIES ('${invalid.key}'='${invalid.value}')
        """
        sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
        test {
            sql "SELECT label, payload FROM csv_two_columns ORDER BY label"
            exception "OpenCSVSerde property '${invalid.key}': ${invalid.message}"
        }
        hive_docker """
            ALTER TABLE csv_table_properties_db.csv_two_columns SET TBLPROPERTIES (
                'separatorChar'='s', 'quoteChar'='q', 'escapeChar'='e'
            )
        """
    }
    // Write with an explicit backslash, then change only the metadata to Hive's double-quote sentinel.
    // This leaves backslash-escaped embedded quotes in the file and exercises Hive's reader constructor choice.
    String sqlBackslash = "\\\\"
    for (boolean tableProperties : [true, false]) {
        String table = tableProperties ? "csv_default_escape_table" : "csv_default_escape_serde"
        hive_docker "DROP TABLE IF EXISTS csv_table_properties_db.${table}"
        hive_docker """
            CREATE TABLE csv_table_properties_db.${table} (label STRING, payload STRING)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            ${tableProperties ? '' : "WITH SERDEPROPERTIES ('escapeChar'='${sqlBackslash}')"}
            STORED AS TEXTFILE
            ${tableProperties ? "TBLPROPERTIES ('escapeChar'='${sqlBackslash}')" : ''}
        """
        hive_docker """
            INSERT INTO csv_table_properties_db.${table}
            SELECT label, payload FROM csv_table_properties_db.source_rows
        """
        for (String sentinel : ['"', '"suffix']) {
            hive_docker """
                ALTER TABLE csv_table_properties_db.${table}
                SET ${tableProperties ? 'TBLPROPERTIES' : 'SERDEPROPERTIES'} ('escapeChar'='${sentinel}')
            """
            sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
            def expected = hive_docker "SELECT label, payload FROM csv_table_properties_db.source_rows ORDER BY label"
            def hiveRows = hive_docker "SELECT label, payload FROM csv_table_properties_db.${table} ORDER BY label"
            assertEquals(expected, hiveRows)
            checkV2 {
                assertEquals(hiveRows, sql("SELECT label, payload FROM ${table} ORDER BY label"))
            }
        }
    }

    // Write raw single-column TEXTFILE rows before switching SerDe. The CSV writer would normalize
    // embedded quotes and hide reader-state bugs; hex literals also preserve binary NULs in fixtures.
    def rawRecords = [
        "x|  qa|bq|c", "abcqleft|rightq|tail", "qleft\nrightq|tail", "left|qunclosed",
        "qaqqbq,tail", "eeabc,tail", "a\u0000b,tail", "", "|", ",", "qleftq|tail",
        "x|\u2003\u2003qa|bq|c", "a\u0000\u0000b,tail", "a\\b,tail",
        '"a""b",tail', '"a\\"b",tail', "abcqleftérightqétail"
    ]
    String rawExpressions = rawRecords.collect {
        "decode(unhex('${it.getBytes('UTF-8').encodeHex()}'), 'UTF-8')"
    }.join(", ")
    // PostgreSQL-backed Hive metastores cannot persist NUL in text properties. Disabled quote/escape
    // settings remain covered by the Hive SerDe oracle and V2 reader unit tests, without a metastore.
    def dialects = [
        [table: "csv_raw_custom", separator: "|", quote: "q", escape: "e"],
        [table: "csv_raw_backslash", separator: ",", quote: "q", escape: sqlBackslash],
        [table: "csv_raw_default", separator: ",", quote: '"', escape: sqlBackslash],
        [table: "csv_raw_utf8", separator: "é", quote: "q", escape: "e"]
    ]
    for (def dialect : dialects) {
        hive_docker "DROP TABLE IF EXISTS csv_table_properties_db.${dialect.table}"
        hive_docker """
            CREATE TABLE csv_table_properties_db.${dialect.table} (raw_record STRING) STORED AS TEXTFILE
        """
        hive_docker """
            INSERT INTO csv_table_properties_db.${dialect.table}
            SELECT explode(array(${rawExpressions}))
        """
        hive_docker """
            ALTER TABLE csv_table_properties_db.${dialect.table}
            REPLACE COLUMNS (first_value STRING, second_value STRING, third_value STRING)
        """
        hive_docker """
            ALTER TABLE csv_table_properties_db.${dialect.table}
            SET SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        """
        hive_docker """
            ALTER TABLE csv_table_properties_db.${dialect.table} SET TBLPROPERTIES (
                'separatorChar'='${dialect.separator}', 'quoteChar'='${dialect.quote}',
                'escapeChar'='${dialect.escape}'
            )
        """
        sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
        for (String query : [
            "SELECT coalesce(hex(first_value), 'NULL'), coalesce(hex(second_value), 'NULL'), " +
                "coalesce(hex(third_value), 'NULL') FROM %s ORDER BY 1, 2, 3",
            "SELECT count(*), count(first_value), count(second_value), count(third_value) FROM %s",
            "SELECT coalesce(hex(third_value), 'NULL'), hex(first_value) FROM %s " +
                "WHERE first_value IS NOT NULL ORDER BY 1, 2"
        ]) {
            def expected = hive_docker(String.format(query, "csv_table_properties_db.${dialect.table}"))
            checkV2 {
                assertEquals(expected, sql(String.format(query, dialect.table)))
            }
        }
    }

    // These metadata values initialize successfully in Hive, but its reader rejects the effective tuple.
    for (String properties : [
        "'separatorChar'='q', 'quoteChar'='q', 'escapeChar'='e'",
        "'separatorChar'='e', 'quoteChar'='q', 'escapeChar'='e'",
        "'separatorChar'='s', 'quoteChar'='e', 'escapeChar'='e'",
        "'separatorChar'='${sqlBackslash}', 'quoteChar'='q', 'escapeChar'='\"'"
    ]) {
        hive_docker """
            ALTER TABLE csv_table_properties_db.csv_two_columns SET TBLPROPERTIES (${properties})
        """
        sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
        test {
            sql "SELECT label, payload FROM csv_two_columns ORDER BY label"
            exception "separatorChar, quoteChar and escapeChar must be distinct when non-NUL"
        }
    }
    hive_docker """
        ALTER TABLE csv_table_properties_db.csv_two_columns SET TBLPROPERTIES (
            'separatorChar'='s', 'quoteChar'='q', 'escapeChar'='e'
        )
    """
    sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"
}
