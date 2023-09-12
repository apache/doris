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

suite("inlineview_with_project") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
        drop table if exists issue_19611_t0;
    """

    sql """
        drop table if exists issue_19611_t1;
    """

    sql """
        create table issue_19611_t0 (c0 int)
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c0) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table issue_19611_t1 (c0 int)
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c0) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    test {
        sql """
             select * from (
                select * from issue_19611_t0, issue_19611_t1 where issue_19611_t1.c0 != 0 
                    union select * from issue_19611_t0, issue_19611_t1 where issue_19611_t1.c0 = 0) tmp;
        """
        exception "Duplicated inline view column alias: 'c0' in inline view: 'tmp'"

    }


    sql """
        drop table if exists issue_19611_t0;
    """

    sql """
        drop table if exists issue_19611_t1;
    """
}
