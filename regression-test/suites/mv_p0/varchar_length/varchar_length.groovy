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

suite ("varchar_length") {

    sql """DROP TABLE IF EXISTS test1; """

    sql """
            CREATE TABLE test1(
            vid VARCHAR(1) NOT NULL COMMENT "",
            report_time int NOT NULL COMMENT ''
            )
            ENGINE=OLAP
            UNIQUE KEY(vid, report_time)
            DISTRIBUTED BY HASH(vid) BUCKETS AUTO
            PROPERTIES
            (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
            ); 
        """
        // only mor table can have mv

    createMV ("CREATE MATERIALIZED VIEW mv_test as SELECT report_time, vid FROM test1 ORDER BY report_time DESC; ")

    qt_select_exp "desc test1 all"
}
