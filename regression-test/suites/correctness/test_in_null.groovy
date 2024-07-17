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

suite("test_in_null") {
     sql """ DROP TABLE IF EXISTS db """
     sql """
        CREATE TABLE IF NOT EXISTS db(
              `id` INT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """
    sql """ INSERT INTO db VALUES(1); """
    sql """ INSERT INTO db VALUES(2); """
    sql """ INSERT INTO db VALUES(3); """
    sql """ INSERT INTO db VALUES(0); """
    sql """ INSERT INTO db VALUES(NULL); """

    qt_select1 """
        select id,id IN (NULL)  from db order by id;
    """
    qt_select2 """
        select id,id in (2,null)  from db order by id;
    """
}
