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

// The cases is copied from 
// https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html
// and modified by Doris.

suite("explode_json_array") {
    def tableName = "person"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} 
        (id INT, name STRING, age INT, class INT, address STRING) 
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8  
        PROPERTIES("replication_num" = "1")
    """

    sql """ INSERT INTO ${tableName} VALUES
        (100, 'John', 30, 1, 'Street 1'),
        (200, 'Mary', NULL, 1, 'Street 2'),
        (300, 'Mike', 80, 3, 'Street 3'),
        (400, 'Dan', 50, 4, 'Street 4')  """
    qt_explode_json_array7 """ SELECT id, name, age, class, address, d_age, c_age FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                        ORDER BY id, c_age, d_age """

    qt_explode_json_array8 """ SELECT c_age, COUNT(1) FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                        GROUP BY c_age ORDER BY c_age """

    qt_explode_json_array9 """ SELECT * FROM ${tableName}
                            LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[]') t1 AS c_age 
                            ORDER BY id, c_age """

    qt_explode_json_array10 """ SELECT id, name, age, class, address, d, c FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", -3]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """

    qt_outer_join_explode_json_array11 """SELECT id, age, e1 FROM (SELECT id, age, e1 FROM (SELECT b.id, a.age FROM 
                                        ${tableName} a LEFT JOIN ${tableName} b ON a.id=b.age)T LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", 3]')
                                        TMP AS e1) AS T ORDER BY age, e1"""
    qt_outer_join_explode_json_array11 """SELECT id, age, e1 FROM (SELECT id, age, e1 FROM (SELECT b.id, a.age FROM
                                        ${tableName} a LEFT JOIN ${tableName} b ON a.id=b.age)T LATERAL VIEW EXPLODE_JSON_ARRAY_JSON('[{"id":1,"name":"John"},{"id":2,"name":"Mary"},{"id":3,"name":"Bob"}]')
                                        TMP AS e1) AS T ORDER BY age, e1"""

    qt_explode_json_array12 """ SELECT c_age, COUNT(1) FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[9223372036854775807,9223372036854775808]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """

    qt_explode_json_array13 """ SELECT c_age, COUNT(1) FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[-92233720368547758071,-92233720368547758081]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """

    qt_explode_json_array14 """ SELECT id, name, age, class, address, d, c FROM ${tableName}
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1182381637816312, "b", -1273982982312333]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """
}
