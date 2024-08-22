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

suite("test_vector_index_nullable"){

    def test_vector_approx_distance_function = { indexTblName, funcName, arr, metricType, order, opcode, range, k ->

        sql "DROP TABLE IF EXISTS ${indexTblName}"

        sql """
                CREATE TABLE if not exists ${indexTblName} (
                     `id` varchar(100) NOT NULL COMMENT "",
                     `pk` bigint(20) NULL COMMENT "",
                     `question` varchar(2048) NOT NULL COMMENT "",
                     `question_embedding` array<float> NULL COMMENT "",
                     `question_embedding2` DECIMAL NOT NULL COMMENT "",
                     INDEX my_index (`question_embedding`) USING VECTOR PROPERTIES(
                         "dim" = "4",
                         "index_type" = "hnsw",
                         "metric_type" = "l2_distance",
                         "efSearch" = "40",
                         "M" = "32",
                         "efConstruction" = "40",
                         "is_vector_normed" = "false"
                     ),
                     INDEX inverted_index (`question_embedding2`) USING INVERTED,
                     )
                     DUPLICATE KEY(`id`)
                     DISTRIBUTED BY HASH(`id`) BUCKETS 1
                     PROPERTIES ("replication_num" = "1");
            """

        sql """
            INSERT INTO ${indexTblName} VALUES
                ('1',1,'我是问题',null,1),
                ('2',2,'我是问题',[0.02,0.03,0.04,0.05],2),
                ('3',3,'我是问题',[0.03,0.04,0.05,0.06],3),
                ('4',4,'我是问题',[0.04,0.05,0.06,0.07],4),
                ('5',5,'我是问题',[0.05,0.06,0.07,0.08],5),
                ('6',6,'我是问题',[0.06,0.07,0.08,0.09],6),
                ('7',7,'我是问题',[0.07,0.08,0.09,0.10],7),
                ('8',8,'我是问题',null,8),
                ('9',9,'我是问题',[0.09,0.10,0.11,0.12],9),
                ('10',10,'我是问题',[0.10,0.11,0.12,0.13],10),
                ('11',11,'我是问题',[0.11,0.12,0.13,0.14],11),
                ('12',12,'我是问题',[0.12,0.13,0.14,0.15],12),
                ('13',13,'我是问题',[0.13,0.14,0.15,0.16],13),
                ('14',14,'我是问题',null,14),
                ('15',15,'我是问题',[0.15,0.16,0.17,0.18],15),
                ('16',16,'我是问题',[0.16,0.17,0.18,0.19],16),
                ('17',17,'我是问题',[0.17,0.18,0.19,0.20],17),
                ('18',18,'我是问题',[0.18,0.19,0.20,0.21],18),
                ('19',19,'我是问题',[0.19,0.20,0.21,0.22],19),
                ('20',20,'我是问题',[0.20,0.21,0.22,0.23],20),
                ('21',21,'我是问题',[0.21,0.22,0.23,0.24],21),
                ('22',22,'我是问题',[0.22,0.23,0.24,0.25],22),
                ('23',23,'我是问题',null,23),
                ('24',24,'我是问题',[0.24,0.25,0.26,0.27],24);
            """

        // order by
        qt_sql "select id from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"
        qt_sql "select id from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"
        qt_sql "select id, $funcName($arr, question_embedding) from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"
        qt_sql "select *, $funcName($arr, question_embedding) from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"
        qt_sql "select $funcName($arr, question_embedding) from $indexTblName order by $funcName($arr, question_embedding) $order limit $k;"

    }

    test_vector_approx_distance_function.call("test_approx_l2_distance", "approx_l2_distance", "[0.01,0.02,0.03,0.04]", "l2_distance", "ASC", "<", 100, 10);
    test_vector_approx_distance_function.call("test_approx_inner_product", "approx_inner_product","[0.01,0.02,0.03,0.04]", "inner_product", "DESC", ">", 100, 20);
    test_vector_approx_distance_function.call("test_approx_cosine_similarity", "approx_cosine_similarity", "[0.01,0.02,0.03,0.04]", "cosine_similarity", "DESC", ">", 0, 15);

}