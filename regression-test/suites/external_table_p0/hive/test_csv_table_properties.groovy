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
            ('2', 'Omega', 'edge, requests', 4),
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
        // Keep these settings ONLY in TBLPROPERTIES: putting them in SERDEPROPERTIES hides the bug.
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

        sql "REFRESH DATABASE csv_table_properties_catalog.csv_table_properties_db"

        // Compare against the source data in Hive, not another CSV read with the same delimiter bug.
        def queries = [
            "SELECT ${layout.projection} FROM %s ORDER BY label",
            "SELECT ${layout.aggregate} FROM %s",
            "SELECT ${layout.projection} FROM %s WHERE label = 'Omega' ORDER BY label"
        ]
        for (String query : queries) {
            def expected = hive_docker(String.format(query, "csv_table_properties_db.source_rows"))
            def actual = sql(String.format(query, layout.table))
            assertEquals(expected, actual)
        }
    }
}
