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

suite("test_mc_write_catalog_block_bytes", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String endpoint = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
    String defaultProject = "mc_doris_test_write"
    String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)

    int rowCount = 128
    String internalDb = "mc_block_bytes_internal_${uuid}"
    String internalTb = "block_bytes_src_${uuid}"

    def normalizeRows = { rows ->
        rows.collect { row ->
            row.collect { value -> value == null ? "null" : value.toString() }
        }
    }

    def expectedSummary = [["128", "0", "127", "2058", "2060"]]
    def expectedDistribution = [
        ["20250101", "32"],
        ["20250102", "32"],
        ["20250103", "32"],
        ["20250104", "32"]
    ]
    def expectedPreview = [
        ["0", "2058", "20250101"],
        ["1", "2058", "20250102"],
        ["2", "2058", "20250103"],
        ["3", "2058", "20250104"],
        ["4", "2058", "20250101"]
    ]
    def catalogConfigs = [
        [
            name: "test_mc_write_catalog_block_bytes_4k_${uuid}",
            maxBlockBytes: "4",
            db: "mc_block_bytes_4k_${uuid}",
            tb: "block_bytes_4k_${uuid}"
        ],
        [
            name: "test_mc_write_catalog_block_bytes_8k_${uuid}",
            maxBlockBytes: "8192",
            db: "mc_block_bytes_8k_${uuid}",
            tb: "block_bytes_8k_${uuid}"
        ],
        [
            name: "test_mc_write_catalog_block_bytes_16k_${uuid}",
            maxBlockBytes: "163840",
            db: "mc_block_bytes_16k_${uuid}",
            tb: "block_bytes_16k_${uuid}"
        ]
    ]
    def createdCatalogConfigs = []

    try {
        sql """CREATE DATABASE IF NOT EXISTS internal.${internalDb}"""
        sql """DROP TABLE IF EXISTS internal.${internalDb}.${internalTb}"""
        sql """
        CREATE TABLE internal.${internalDb}.${internalTb} (
            id INT,
            payload STRING,
            ds STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql """
        INSERT INTO internal.${internalDb}.${internalTb}
        SELECT
            number AS id,
            concat('payload_', cast(number AS STRING), '_', repeat('x', 2048)) AS payload,
            concat('2025010', cast((number % 4 + 1) AS STRING)) AS ds
        FROM numbers("number"="${rowCount}")
        """

        for (def catalogConfig in catalogConfigs) {
            String catalogName = catalogConfig.name
            String maxBlockBytes = catalogConfig.maxBlockBytes
            String db = catalogConfig.db
            String tb = catalogConfig.tb

            sql """DROP CATALOG IF EXISTS ${catalogName}"""
            sql """
            CREATE CATALOG IF NOT EXISTS ${catalogName} PROPERTIES (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "${endpoint}",
                "mc.quota" = "pay-as-you-go",
                "mc.enable.namespace.schema" = "true",
                "mc.write_max_block_bytes" = "${maxBlockBytes}"
            );
            """
            createdCatalogConfigs.add(catalogConfig)

            def showCreate = sql """SHOW CREATE CATALOG ${catalogName}"""
            assert showCreate.size() == 1
            assert showCreate[0][1].contains("\"mc.write_max_block_bytes\" = \"${maxBlockBytes}\"")

            sql """SWITCH ${catalogName}"""
            sql """DROP DATABASE IF EXISTS ${db}"""
            sql """CREATE DATABASE ${db}"""
            sql """USE ${db}"""
            sql """DROP TABLE IF EXISTS ${tb}"""
            sql """
            CREATE TABLE ${tb} (
                id INT,
                payload STRING,
                ds STRING
            )
            """

            sql """INSERT INTO ${tb} SELECT * FROM internal.${internalDb}.${internalTb}"""

            def summary = normalizeRows(sql """
                SELECT count(*), min(id), max(id), min(length(payload)), max(length(payload))
                FROM ${tb}
            """)
            assert summary == expectedSummary : "${catalogName} summary mismatch: ${summary}"

            def distribution = normalizeRows(sql """
                SELECT ds, count(*)
                FROM ${tb}
                GROUP BY ds
                ORDER BY ds
            """)
            assert distribution == expectedDistribution : "${catalogName} distribution mismatch: ${distribution}"

            def preview = normalizeRows(sql """
                SELECT id, length(payload), ds
                FROM ${tb}
                ORDER BY id
                LIMIT 5
            """)
            assert preview == expectedPreview : "${catalogName} preview mismatch: ${preview}"
        }
    } finally {
        for (int i = createdCatalogConfigs.size() - 1; i >= 0; i--) {
            def catalogConfig = createdCatalogConfigs[i]
            sql """DROP DATABASE IF EXISTS ${catalogConfig.name}.${catalogConfig.db}"""
            sql """DROP CATALOG IF EXISTS ${catalogConfig.name}"""
        }
    }
}
