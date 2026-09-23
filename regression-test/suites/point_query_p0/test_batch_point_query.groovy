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

suite("test_batch_point_query", "p0") {
    sql "DROP TABLE IF EXISTS batch_address_graph"
    sql """
        CREATE TABLE batch_address_graph (
            address VARCHAR(128) NOT NULL,
            out_edges_json TEXT NULL,
            in_edges_json TEXT NULL,
            out_degree BIGINT NULL,
            in_degree BIGINT NULL,
            total_out_usd DOUBLE NULL,
            total_in_usd DOUBLE NULL,
            rank_updated_at VARCHAR(64) NULL
        ) UNIQUE KEY(address)
        DISTRIBUTED BY HASH(address) BUCKETS 128
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "row_store_page_size" = "16384"
        )
    """
    def addresses = [
        '0x6b7c4fc6b0751ad1f5e9a04c483534932396586b',
        '0x4c68ef446bd8a3be019d090ec3184d4416e1cad2',
        '0x9c96f35f7908259148472cd86f56ce9d706adc2a',
        '0x2744dfd9898f0babbc570cc594bbbc84b487a22b',
        '0xacd445b47b3dcb791f92a480e99c24efa907ab62',
        '0x79aa857aeb9f9e707b372ad3c2b2bdd83d7416c9',
        '0xae2d4617c862309a3d75a0ffb358c7a5009c673f',
        '0xfa219fb098a2653f88707495f9a4464aee6ac07f',
        '0xcdfa8caf936898cd4dad182723603c628943d95c'
    ]
    addresses.eachWithIndex { address, i ->
        sql """INSERT INTO batch_address_graph VALUES
            ('${address}', '[{"address":"edge_${i}"}]', NULL, ${i}, 0, ${i}.5, NULL, '2026-09-23')"""
    }
    def projection = "address,out_edges_json,in_edges_json,out_degree,in_degree,total_out_usd,total_in_usd,rank_updated_at"
    def addressList = addresses.collect { "'${it}'" }.join(',')
    def query = "SELECT ${projection} FROM batch_address_graph WHERE address IN (${addressList})"
    sql "SET enable_batch_point_query=false"
    explain { sql query; notContains "SHORT-CIRCUIT" }
    order_qt_scan_nine query
    sql "SET enable_batch_point_query=true"
    explain { sql query; contains "SHORT-CIRCUIT" }
    order_qt_batch_nine query
    order_qt_batch_nine_repeat query
    order_qt_duplicate_and_absent """SELECT ${projection} FROM batch_address_graph
        WHERE address IN ('${addresses[0]}','${addresses[0]}','${addresses[1]}','missing')"""

    sql """INSERT INTO batch_address_graph
        SELECT concat('generated_',number), concat('[{"data":"',repeat('x',4096),'"}]'),
               '[]',number,0,number,0,'2026-09-23'
        FROM numbers("number"="100")"""
    def hundred = (0..<100).collect { "'generated_${it}'" }.join(',')
    // Project only stored columns, so all 100 keys actually enter the batch path.
    def hundredQuery = "SELECT address,out_degree,in_degree FROM batch_address_graph WHERE address IN (${hundred})"
    explain { sql hundredQuery; contains "SHORT-CIRCUIT" }
    order_qt_batch_hundred hundredQuery
    sql "SET enable_batch_point_query=false"
    order_qt_scan_hundred hundredQuery
    def wideHundredQuery = "SELECT ${projection} FROM batch_address_graph WHERE address IN (${hundred})"
    def scanWideRows = sql(wideHundredQuery).sort { a, b -> a[0] <=> b[0] }
    sql "SET enable_batch_point_query=true"
    explain { sql wideHundredQuery; contains "SHORT-CIRCUIT" }
    def batchWideRows = sql(wideHundredQuery).sort { a, b -> a[0] <=> b[0] }
    // Differential check avoids a large output file while comparing every byte of both wide JSON columns.
    assertEquals(scanWideRows, batchWideRows)
    explain {
        sql "SELECT address FROM batch_address_graph WHERE address IN (${hundred},'over_limit')"
        notContains "SHORT-CIRCUIT"
    }
    explain {
        sql "SELECT address FROM batch_address_graph WHERE address IN (${addressList}) AND out_degree>3"
        notContains "SHORT-CIRCUIT"
    }

    // Force several keys into the same request and cover mixed live/deleted rows.
    sql "DROP TABLE IF EXISTS batch_address_one_bucket"
    sql """CREATE TABLE batch_address_one_bucket(address VARCHAR(128) NOT NULL,value STRING NULL)
        UNIQUE KEY(address) DISTRIBUTED BY HASH(address) BUCKETS 1
        PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true","store_row_column"="true")"""
    sql "INSERT INTO batch_address_one_bucket VALUES ('a','old'),('b','deleted'),('c',NULL)"
    sql "INSERT INTO batch_address_one_bucket VALUES ('a','new')"
    sql "DELETE FROM batch_address_one_bucket WHERE address='b'"
    explain {
        sql "SELECT address,value FROM batch_address_one_bucket WHERE address IN ('a','b','c','missing')"
        contains "SHORT-CIRCUIT"
    }
    order_qt_batch_mixed_delete "SELECT address,value FROM batch_address_one_bucket WHERE address IN ('a','b','c','missing')"
    sql "SET enable_batch_point_query=false"
    order_qt_scan_mixed_delete "SELECT address,value FROM batch_address_one_bucket WHERE address IN ('a','b','c','missing')"
}
