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

suite("docs/table-design/index/bloomfilter.md") {
    try {
        multi_sql """
        CREATE TABLE IF NOT EXISTS sale_detail_bloom  (
            sale_date date NOT NULL COMMENT "Sale date",
            customer_id int NOT NULL COMMENT "Customer ID",
            saler_id int NOT NULL COMMENT "Salesperson",
            sku_id int NOT NULL COMMENT "Product ID",
            category_id int NOT NULL COMMENT "Product category",
            sale_count int NOT NULL COMMENT "Sales quantity",
            sale_price DECIMAL(12,2) NOT NULL COMMENT "Unit price",
            sale_amt DECIMAL(20,2)  COMMENT "Total sales amount"
        )
        DUPLICATE KEY(sale_date, customer_id, saler_id, sku_id, category_id)
        DISTRIBUTED BY HASH(saler_id) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1",
        "bloom_filter_columns"="saler_id,category_id"
        );
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/index/bloomfilter.md failed to exec, please fix it", t)
    }
}
