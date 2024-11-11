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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/update/update-of-aggregate-model.md", "p0,nonConcurrent") {
    try {
        multi_sql """
            DROP TABLE IF EXISTS order_tbl;
            CREATE TABLE order_tbl (
              order_id int(11) NULL,
              order_amount int(11) REPLACE_IF_NOT_NULL NULL,
              order_status varchar(100) REPLACE_IF_NOT_NULL NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(order_id)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(order_id) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
            INSERT INTO order_tbl(order_id, order_amount, order_status) VALUES
                                 (1       , 100         , 'Pending'   );
        """

        cmd """curl  --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns:order_id,order_status" -T ${context.file.parent}/update.csv http://${context.config.feHttpAddress}/api/${curDbName}/order_tbl/_stream_load"""
        sql """INSERT INTO order_tbl (order_id, order_status) values (1,'Delivery Pending');"""
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/update/update-of-aggregate-model.md failed to exec, please fix it", t)
    }
}
