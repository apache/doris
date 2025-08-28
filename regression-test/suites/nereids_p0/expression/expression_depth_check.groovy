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

suite("expression_depth_check", "nonConcurrent") {
    sql """
        drop table if exists tbl1;
        CREATE TABLE tbl1
        (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 6
        PROPERTIES
        (
            "replication_num" = "1"
        );
        select floor(abs(ceil(1+k1))) from tbl1;
    """
    sql """
        admin set frontend config("expr_depth_limit" = "3");
    """
    def depthCheckFailed = true;
    try {
        sql """ 
        select floor(abs(ceil(1+k1))) from tbl1
        """
    } catch (Exception e) {
        if (e.getMessage().contains("Exceeded the maximum depth of an expression tree (3)")) {
            depthCheckFailed = false;
        }
    } finally {
        sql """
        admin set frontend config("expr_depth_limit" = "3000");
        """
        if (depthCheckFailed) {
            throw new RuntimeException("check expression depth failed")
        }
    }

     

}