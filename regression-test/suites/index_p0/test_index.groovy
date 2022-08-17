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
suite("test_index") {
    // todo: test bitmap index, such as create, drop, alter table index
    sql "DROP TABLE IF EXISTS t1"
    sql """
    CREATE TABLE IF NOT EXISTS t1 (
        col1 int not null,
        col2 char(10)
    )DISTRIBUTED BY HASH(col1) BUCKETS 5 properties("replication_num" = "1");
    """
    def tables = sql "show tables"
    def tb = tables[0][0]
    logger.info("$tb")
    sql "show index from ${tables[0][0]}"
}
