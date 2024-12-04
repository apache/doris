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


suite("test_inverted_index_v3_fault_injection", "nonConcurrent"){
    def indexTbName1 = "test_inverted_index_v3_fault_injection"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX clientip_idx (`clientip`) COMMENT '',
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "inverted_index_storage_format" = "V3"
      );
    """

    try {
      GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_v3")
      
      sql """ INSERT INTO ${indexTbName1} VALUES (1, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
    } finally {
      GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_v3")
    }

    try {
      GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_v3")
      GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_dic_compression")

      sql """ INSERT INTO ${indexTbName1} VALUES (2, '40.135.0.0', 'GET /images/hm_bg.jpg HTTP/1.0', 200, 24736); """
    } finally {
      GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_v3")
      GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::create_field_dic_compression")
    }
}