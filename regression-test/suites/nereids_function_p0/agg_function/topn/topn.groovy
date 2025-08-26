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

suite("topn") {
    sql """ DROP TABLE IF EXISTS test_topn """

    sql """
        CREATE TABLE test_topn (
          `id` int,
          `x` int,
          `y` int,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    
    // Perfect positive correlation    
    sql """
        insert into test_topn values
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
        (5, 5, 5)
        """
   
    test {
        sql """select topn(id,id) from test_topn;"""
    	exception "errCode = 2"
    }

    test {
        sql """select topn(id,1,id) from test_topn;"""
    	exception "errCode = 2"
    }

    test {
        sql """select topn_array(id,id) from test_topn;"""
    	exception "errCode = 2"
    }
    test {
        sql """select topn_array(id,1,id) from test_topn;"""
    	exception "errCode = 2"
    }

    test {
        sql """select topn_weighted(id,id,id) from test_topn;"""
    	exception "errCode = 2"
    }
    test {
        sql """select topn_weighted(id,id,1,id) from test_topn;"""
    	exception "errCode = 2"
    }
}
