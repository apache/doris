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

suite("test_similar_query_with_different_results") {
    def tableName = "t0"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c0 BOOLEAN DEFAULT 'false'
            )
            DISTRIBUTED BY RANDOM
            PROPERTIES ("replication_num" = "1")
        """

    sql """ INSERT INTO ${tableName} (c0) VALUES 
            (DEFAULT),('false'),('true'),(DEFAULT),('true'),(DEFAULT),(DEFAULT),('false')
        """
    qt_select1 """ SELECT c0 FROM ${tableName} ORDER BY c0 """
    qt_select2 """ SELECT c0 FROM ${tableName} WHERE c0 """
    qt_select3 """ SELECT c0 FROM ${tableName} WHERE (NOT c0) """
    qt_select4 """ SELECT c0 FROM ${tableName} WHERE ((c0) IS NULL) """
    qt_select5 """ SELECT RES.c0 FROM (
                        SELECT c0 FROM ${tableName} WHERE c0
                        UNION ALL
                        SELECT c0 FROM ${tableName} WHERE (NOT c0)
                        UNION ALL
                        SELECT c0 FROM ${tableName} WHERE ((c0) IS NULL)
                   ) AS RES
                   ORDER BY RES.c0 DESC
               """

    sql""" DROP TABLE IF EXISTS ${tableName} """
}