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

suite("test_upgrade_downgrade_compatibility_inverted_index","p0,inverted_index,restart_fe") {
    def timeout = 120000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    sql "SET enable_common_expr_pushdown = true"
    sql "SET enable_match_without_inverted_index = false"

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    for (version in ["V1", "V2"]) {
        // duplicate table
        def tableName = "t_up_down_inverted_index${version}_duplicate"
        def result = sql(String.format("show create table %s", tableName))
        if (!result[0][1].toString().contains(String.format('''"inverted_index_storage_format" = "%s"''', version))) {
            throw new IllegalStateException("${tableName} inverted_index_storage_format exception")
        }
        result = sql("select count(*) from ${tableName}")
        if (result[0][0] != 10) {
            throw new IllegalStateException("count error")
        }
        result = sql("select count(*) from ${tableName} where en in ('I see', 'Let go')")
        if (result[0][0] != 2) {
            throw new IllegalStateException("count error")
        }
        result = sql("select count(*) from ${tableName} where en match_any ('on')")
        if (result[0][0] != 2) {
            throw new IllegalStateException("count error")
        }
        result = sql("select count(*) from ${tableName} where ch = '等一等'")
        if (result[0][0] != 1) {
            throw new IllegalStateException("count error")
        }

        sql "alter table ${tableName} add index idx_b(b)"
        wait_for_latest_op_on_table_finish(tableName, timeout)
        sql "build index idx_b on ${tableName}"
        wait_for_build_index_on_partition_finish(tableName, timeout)

        sql "insert into ${tableName} values(10001, 10001, 10001, 'Not yet', '还没')"
        sql "insert into ${tableName} values(10002, 10002, 10002, 'So long', '再见')"
        sql "insert into ${tableName} values(10003, 10003, 10003, 'Why not', '为什么不')"
        result = sql("select a, b, c from ${tableName} where a = 10001")
        if (result.size() != 1 || result[0][0] != 10001 || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        result = sql("select a, b, c from ${tableName} where b = 10001")
        if (result.size() != 1 || result[0][0] != 10001 || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        sql "delete from ${tableName} where a = 10001"
        sql "delete from ${tableName} where b = 10002"
        sql "delete from ${tableName} where c = 10003"
        for (String columnName in ["a", "b", "c"]) {
            for (String columnValue in ["10001", "10002", "10003"]) {
                result = sql("select ${columnName} from ${tableName} where ${columnName} = ${columnValue}")
                if (result.size() != 0) {
                    throw new IllegalStateException("${result.size()}")
                }
            }
        }
        sql "alter table ${tableName} drop index idx_b"

        // unique table
        tableName = "t_up_down_inverted_index${version}_unique"
        result = sql(String.format("show create table %s", tableName))
        if (!result[0][1].toString().contains(String.format('''"inverted_index_storage_format" = "%s"''', version))) {
            throw new IllegalStateException("${tableName} inverted_index_storage_format exception")
        }
        result = sql("select a, b, c from ${tableName} where a = 10")
        if (result.size() != 1 || result[0][0] != 10 || result[0][1] != 10 || result[0][2] != 10) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        result = sql("select count(*) from ${tableName} where d = '2022-10-22'")
        if (result[0][0] != 1) {
            throw new IllegalStateException("count err")
        }

        sql "alter table ${tableName} add index idx_b(b)"
        sql "alter table ${tableName} add index idx_en(en) using inverted properties(\"parser\" = \"english\", \"support_phrase\" = \"true\")"
        wait_for_latest_op_on_table_finish(tableName, timeout)
        sql "build index idx_b on ${tableName}"
        sql "build index idx_en on ${tableName}"
        wait_for_build_index_on_partition_finish(tableName, timeout)
        sql "insert into ${tableName} values(10001, 10001, 10001, '2024-1-1', 'Not yet', '还没')"
        sql "insert into ${tableName} values(10002, 10002, 10002, '2024-2-1', 'So long', '再见')"
        sql "insert into ${tableName} values(10003, 10003, 10003, '2024-3-1', 'Why not', '为什么不')"
        result = sql("select a, b, c from ${tableName} where a = 10001")
        if (result.size() != 1 || result[0][0] != 10001 || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        result = sql("select a, b, c from ${tableName} where b = 10001")
        if (result.size() != 1 || result[0][0] != 10001 || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        result = sql("select count(*) from ${tableName} where ch match_any('什么')")
        if (result[0][0] != 1) {
            throw new IllegalStateException("count err")
        }
        result = sql("select count(*) from ${tableName} where en MATCH_PHRASE_PREFIX('lon')")
        if (result[0][0] != 1) {
            throw new IllegalStateException("count err")
        }

        sql "delete from ${tableName} partition p3 where a = 10001"
        sql "delete from ${tableName} partition p3 where b = 10002"
        sql "delete from ${tableName} partition p3 where c = 10003"
        for (String columnName in ["a", "b", "c"]) {
            for (String columnValue in ["10001", "10002", "10003"]) {
                result = sql("select ${columnName} from ${tableName} where ${columnName} = ${columnValue}")
                if (result.size() != 0) {
                    throw new IllegalStateException("${result.size()}")
                }
            }
        }
        sql "alter table ${tableName} drop index idx_b"
        sql "alter table ${tableName} drop index idx_en"

        // agg table
        tableName = "t_up_down_inverted_index${version}_agg"
        result = sql(String.format("show create table %s", tableName))
        if (!result[0][1].toString().contains(String.format('''"inverted_index_storage_format" = "%s"''', version))) {
            throw new IllegalStateException("${tableName} inverted_index_storage_format exception")
        }
        result = sql("select a,b,c from ${tableName} where a = \"10\"")
        if (result.size() != 1 || result[0][0] != "10" || result[0][1] != 10 || result[0][2] != 10) {
            throw new IllegalStateException(String.format("%s %d %d", result[0][0], result[0][1], result[0][2]))
        }
        sql "alter table ${tableName} add index idx_b(b)"
        wait_for_latest_op_on_table_finish(tableName, timeout)
        sql "build index idx_b on ${tableName}"
        wait_for_build_index_on_partition_finish(tableName, timeout)
        sql "show index from ${tableName}"
        sql "insert into ${tableName} values(\"10001\", 10001, 10001, 10001, 10001, 10001)"
        sql "insert into ${tableName} values(\"10002\", 10002, 10002, 10002, 10002, 10002)"
        sql "insert into ${tableName} values(\"10003\", 10003, 10003, 10003, 10003, 10003)"
        result = sql("select a, b, c from ${tableName} where a = \"10001\"")
        if (result.size() != 1 || result[0][0] != "10001" || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        result = sql("select a, b, c from ${tableName} where b = 10001")
        if (result.size() != 1 || result[0][0] != "10001" || result[0][1] != 10001 || result[0][2] != 10001) {
            throw new IllegalStateException(String.format("%d %d %d", result[0][0], result[0][1], result[0][2]))
        }
        sql "delete from ${tableName} where a = \"10001\""
        sql "delete from ${tableName} where b = 10002"
        sql "delete from ${tableName} where c = 10003"
        result = sql("select a, b, c from ${tableName} where a = \"10001\"")
        if (result.size() != 0) {
            throw new IllegalStateException(String.format("%d", result.size()))
        }
        for (String columnName in ["b", "c"]) {
            for (String columnValue in ["10002", "10003"]) {
                result = sql("select ${columnName} from ${tableName} where ${columnName} = ${columnValue}")
                if (result.size() != 0) {
                    throw new IllegalStateException("${result.size()}")
                }
            }
        }
        sql "alter table ${tableName} drop index idx_b"
    }
}
