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

suite("test_keywords") {
    // DEFAULT is a non-reserved keyword
    sql """
    drop table if exists DEFAULT;
    """

    sql """
            CREATE TABLE IF NOT EXISTS DEFAULT(
                id int
            )
            DISTRIBUTED BY HASH(id) properties("replication_num" = "1");
        """

    sql """ DESCRIBE DEFAULT; """

    sql """ insert into DEFAULT values(1) """

    test {
        sql "select * from DEFAULT"
        result([[1]])
    }

    sql """ truncate table DEFAULT; """

    sql """ set query_timeout = DEFAULT; """
}
