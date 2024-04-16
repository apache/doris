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

suite('test_alter_force_property') {
    def tbl = 'test_alter_force_property_tbl'
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl}
        (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 6
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    if (isCloudMode()) {
        test {
            sql "ALTER TABLE ${tbl} SET ('default.replication_num' = '1')"
            exception "Cann't modify property 'default.replication_allocation' in cloud mode."
        }
        test {
            sql "ALTER TABLE ${tbl} MODIFY PARTITION (*) SET ('replication_num' = '1')"
            exception "Cann't modify property 'replication_num' in cloud mode."
        }
    } else {
        sql "ALTER TABLE ${tbl} SET ('default.replication_num' = '1')"
        sql "ALTER TABLE ${tbl} MODIFY PARTITION (*) SET ('replication_num' = '1')"
    }

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
