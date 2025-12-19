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

suite("build_ann_index_test") {
    if (isCloudMode()) {
        return // TODO enable this case after enable light index in cloud mode
    }

    // prepare test table
    def timeout = 30000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { tableName, opTimeout ->
        for(int t = delta_time; t <= opTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= opTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_last_build_index_on_table_finish = { tableName, opTimeout ->
        for(int t = delta_time; t <= opTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${tableName}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                logger.info(tableName + " last index job finished")
                return "SKIPPED"
            }
            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    sleep(3000) // wait change table state to normal
                    logger.info(tableName + " last index job finished, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= opTimeout, "wait_for_last_build_index_on_table_finish timeout")
        return "wait_timeout"
    }

    sql "set enable_common_expr_pushdown=true;"
    sql "drop table if exists table_build_ann_index_test;"
    def tableName = "table_build_ann_index_test"

    // case 1: create table -- insert data -- create index -- build index
    sql """
    CREATE TABLE `table_build_ann_index_test` (
      `id` int NOT NULL COMMENT "",
      `embedding` array<float> NOT NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`) COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 2
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    INSERT INTO table_build_ann_index_test (id, embedding) VALUES
        (0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258]),
        (1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428]),
        (2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976]),
        (3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693]),
        (4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834]),
        (5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377]),
        (6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207]),
        (7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409]),
        (8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877]),
        (9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396]);
    """

    // CREATE INDEX
    sql """
    CREATE INDEX idx_test_ann ON table_build_ann_index_test(`embedding`) USING ANN PROPERTIES(
        "index_type"="hnsw",
        "metric_type"="l2_distance",
        "dim"="8"
    );
    """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // BUILD INDEX
    sql "BUILD INDEX idx_test_ann ON table_build_ann_index_test;"
    wait_for_last_build_index_on_table_finish(tableName, timeout)
}
