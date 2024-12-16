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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_index_ddl_fault_injection", "nonConcurrent") {
    try {
      sql "DROP TABLE IF EXISTS `test_index_ddl_fault_injection_tbl`"
      sql """
        CREATE TABLE test_index_ddl_fault_injection_tbl (
          `k1` int(11) NULL COMMENT "",
          `k2` int(11) NULL COMMENT "",
          `v1` string NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`k1`)
          DISTRIBUTED BY HASH(`k1`) BUCKETS 1
          PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
      """

      sql """ INSERT INTO test_index_ddl_fault_injection_tbl VALUES (1, 2, "hello"), (3, 4, "world"); """
      sql 'sync'

      qt_order1 """ select * from test_index_ddl_fault_injection_tbl where v1 = 'hello'; """

      // add bloom filter
      sql """ ALTER TABLE test_index_ddl_fault_injection_tbl set ("bloom_filter_columns" = "v1"); """
      assertEquals("FINISHED", getAlterColumnFinalState("test_index_ddl_fault_injection_tbl"))

      try {
          qt_order2 """ select * from test_index_ddl_fault_injection_tbl where v1 = 'hello'; """
          GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexReader::new_iterator.fail");
          test {
            // if BE add bloom filter correctly, this query will call BloomFilterIndexReader::new_iterator
            sql """ select * from test_index_ddl_fault_injection_tbl where v1 = 'hello'; """
           exception "new_iterator for bloom filter index failed"
          }
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexReader::new_iterator.fail");
      }

      // drop bloom filter
      sql """ ALTER TABLE test_index_ddl_fault_injection_tbl set ("bloom_filter_columns" = ""); """
      assertEquals("FINISHED", getAlterColumnFinalState("test_index_ddl_fault_injection_tbl"))

      try {
          qt_order3 """ select * from test_index_ddl_fault_injection_tbl where v1 = 'hello'; """
          GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexReader::new_iterator.fail");
            // if BE drop bloom filter correctly, this query will not call BloomFilterIndexReader::new_iterator
          qt_order4 """ select * from test_index_ddl_fault_injection_tbl where v1 = 'hello'; """
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexReader::new_iterator.fail");
      }
    } finally {
    }
}
