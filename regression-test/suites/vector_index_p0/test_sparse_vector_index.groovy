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
// specific language governing permissions and LIMITations
// under the License.

suite("test_sparse_vector_index"){
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "sparse_vector_index_test"
    def funcName = "approx_inner_product"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
        id BIGINT,
        content_embedding MAP<int, float> NULL,
        INDEX my_index (`content_embedding`) USING VECTOR PROPERTIES(
            "index_type" = "sparse_wand",
            "metric_type" = "inner_product",
            "drop_ratio_search" = "0.1"
        )
    )
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql """
    INSERT INTO ${indexTblName} VALUES
    (0, null),
    (1, null),
    (2, null),
    (90,{0:0.1,110:0.2,120:0.3}),
    (91,{1:0.2,101:0.2,201:0.3}),
    (92,{2:0.3,201:0.2,202:0.3});
    """

    qt_sql "select *, $funcName({0:0.1,1:0.2,2:0.3}, content_embedding) as similarity from $indexTblName order by similarity desc limit 3;"
    qt_sql "select * from $indexTblName order by $funcName({0:0.1,1:0.2,2:0.3}, content_embedding) desc limit 3;"

}
