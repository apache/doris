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

suite("test_paimon_deletion_vector", "p0,external") {

    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    try {
        String catalog_name = "test_paimon_deletion_vector"
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type" = "paimon",
            "paimon.catalog.type"="filesystem",
            "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
        );"""
        sql """use `${catalog_name}`.`db1`"""

        def test_cases = { String force ->
            sql """ set force_jni_scanner=${force} """
            // Force tiny splits so both native and JNI paths read Paimon DV tables through multiple
            // scanner ranges while still returning the same filtered rows.
            sql """ set file_split_size=1 """
            qt_1 """select count(*) from deletion_vector_orc;"""
            qt_2 """select count(*) from deletion_vector_parquet;"""
            qt_3 """select count(*) from deletion_vector_orc where id > 2;"""
            qt_4 """select count(*) from deletion_vector_parquet where id > 2;"""
            qt_5 """select * from deletion_vector_orc where id > 2 order by id;"""
            qt_6 """select * from deletion_vector_parquet where id > 2 order by id;"""
            qt_7 """select * from deletion_vector_table_1_0 order by id;"""
            qt_8 """select count(*) from deletion_vector_table_1_0;"""
            qt_9 """select count(*) from deletion_vector_table_1_0 where id > 2;"""
            qt_10 """select * from deletion_vector_orc where id > 2 order by id limit 1;"""
            qt_11 """select * from deletion_vector_parquet where id > 2 order by id limit 1;"""
            qt_12 """select count(*) from (select * from deletion_vector_orc where id > 2 union all select * from deletion_vector_orc where id > 2) t;"""
            qt_13 """select count(*) from (select * from deletion_vector_parquet where id > 2 union all select * from deletion_vector_parquet where id > 2) t;"""
        }

        def test_table_count_push_down = { String force ->
            sql """ set force_jni_scanner=${force} """
            // DV tables cannot use table-level count pushdown because deleted rows must be applied;
            // the no-DV v1.0 table keeps the positive pushdown baseline.
            explain {
                sql("select count(*) from deletion_vector_orc;")
                contains "pushdown agg=COUNT (-1)"
            }
            explain {
                sql("select count(*) from deletion_vector_parquet;")
                contains "pushdown agg=COUNT (-1)"
            }
            explain {
                sql("select count(*) from deletion_vector_table_1_0;")
                contains "pushdown agg=COUNT (8)"
            }
        }

        def test_not_table_count_push_down = { String force ->
            sql """ set enable_count_push_down_for_external_table=false; """
            sql """ set force_jni_scanner=${force} """
            // With count pushdown disabled, all DV and non-DV tables must report NONE regardless
            // of the selected scanner path.
            explain {
                sql("select count(*) from deletion_vector_orc;")
                contains "pushdown agg=NONE"
            }
            explain {
                sql("select count(*) from deletion_vector_parquet;")
                contains "pushdown agg=NONE"
            }
            explain {
                sql("select count(*) from deletion_vector_table_1_0;")
                contains "pushdown agg=NONE"
            }
        }

        test_cases("false")
        test_cases("true")
        test_table_count_push_down("false")
        test_table_count_push_down("true")
        test_not_table_count_push_down("false")
        test_not_table_count_push_down("true")
    } finally {
        sql """ set enable_count_push_down_for_external_table=true; """
        sql """unset variable file_split_size"""
        sql """set force_jni_scanner=false"""
    }

    sql "CREATE DATABASE IF NOT EXISTS test_paimon_deletion_vector.paimon_bitmap64_dv_db"
    sql "DROP TABLE IF EXISTS test_paimon_deletion_vector.paimon_bitmap64_dv_db.bitmap64_dv"
    sql """
        CREATE TABLE test_paimon_deletion_vector.paimon_bitmap64_dv_db.bitmap64_dv (
            id INT NOT NULL, payload INT
        ) ENGINE=paimon PROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'file.format' = 'parquet',
            'deletion-vectors.enabled' = 'true',
            'deletion-vectors.bitmap64' = 'true'
        )
    """
    sql "INSERT INTO test_paimon_deletion_vector.paimon_bitmap64_dv_db.bitmap64_dv VALUES (1, 10), (2, 20), (3, 30)"
    sql "DELETE FROM test_paimon_deletion_vector.paimon_bitmap64_dv_db.bitmap64_dv WHERE id = 2"

    // The native query must read a physical DV rather than an already rewritten data file.
    def indexes = sql """
        SELECT row_count
        FROM test_paimon_deletion_vector.paimon_bitmap64_dv_db.`bitmap64_dv\$table_indexes`
        WHERE index_type = 'DELETION_VECTORS' AND row_count > 0
    """
    assertFalse(indexes.isEmpty(), "DELETE must leave a deletion-vector entry")

    def bitmap64Query = """
        SELECT /*+ SET_VAR(enable_file_scanner_v2=true, enable_paimon_rust_reader=false, force_jni_scanner=false) */
               id, payload
        FROM test_paimon_deletion_vector.paimon_bitmap64_dv_db.bitmap64_dv
        ORDER BY id
    """
    explain {
        sql bitmap64Query
        contains "paimonNativeReadSplits=1/1"
    }
    order_qt_bitmap64_native bitmap64Query
}
