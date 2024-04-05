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

suite("test_join2", "query,p0,arrow_flight_sql") {
    def DBname = "regression_test_join2"
    def TBname1 = "J1_TBL"
    def TBname2 = "J2_TBL"

    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "create database IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"

    sql "DROP TABLE IF EXISTS ${TBname1}"
    sql "DROP TABLE IF EXISTS ${TBname2}"

    sql """
            CREATE TABLE IF NOT EXISTS ${TBname1} (
                i integer,
                j integer,
                t text
            )
            DISTRIBUTED BY HASH(i) properties("replication_num" = "1");
        """
    sql """
            CREATE TABLE IF NOT EXISTS ${TBname2} (
                i integer,
                k integer
            )
            DISTRIBUTED BY HASH(i) properties("replication_num" = "1");
        """

    sql "INSERT INTO ${TBname1} VALUES (1, 4, 'one');"
    sql "INSERT INTO ${TBname1} VALUES (2, 3, 'two');"
    sql "INSERT INTO ${TBname1} VALUES (3, 2, 'three');"
    sql "INSERT INTO ${TBname1} VALUES (4, 1, 'four');"
    sql "INSERT INTO ${TBname1} VALUES (5, 0, 'five');"
    sql "INSERT INTO ${TBname1} VALUES (6, 6, 'six');"
    sql "INSERT INTO ${TBname1} VALUES (7, 7, 'seven');"
    sql "INSERT INTO ${TBname1} VALUES (8, 8, 'eight');"
    sql "INSERT INTO ${TBname1} VALUES (0, NULL, 'zero');"
    sql "INSERT INTO ${TBname1} VALUES (NULL, NULL, 'null');"
    sql "INSERT INTO ${TBname1} VALUES (NULL, 0, 'zero');"
    sql "INSERT INTO ${TBname2} VALUES (1, -1);"
    sql "INSERT INTO ${TBname2} VALUES (2, 2);"
    sql "INSERT INTO ${TBname2} VALUES (3, -3);"
    sql "INSERT INTO ${TBname2} VALUES (2, 4);"
    sql "INSERT INTO ${TBname2} VALUES (5, -5);"
    sql "INSERT INTO ${TBname2} VALUES (5, -5);"
    sql "INSERT INTO ${TBname2} VALUES (0, NULL);"
    sql "INSERT INTO ${TBname2} VALUES (NULL, NULL);"
    sql "INSERT INTO ${TBname2} VALUES (NULL, 0);"

    qt_join1 """
            SELECT '' AS "xxx", *
            FROM ${TBname1}
            INNER JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join2 """
            SELECT '' AS "xxx", *
            FROM ${TBname1}
            JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    test {
        sql """
            SELECT '' AS "xxx", *
            FROM ${TBname1} NATURAL JOIN ${TBname2}
            ORDER BY 1,2,3,4,5,6;
        """
        exception "natural join is not supported"
    }
    
    qt_join4 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} JOIN ${TBname2}
            ON (${TBname1}.i = ${TBname2}.i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join5 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} JOIN ${TBname2}
            ON (${TBname1}.i = ${TBname2}.k)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join5 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} JOIN ${TBname2}
            ON (${TBname1}.i <= ${TBname2}.k)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join6 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} LEFT OUTER JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join7 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} LEFT JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join8 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} RIGHT
            OUTER JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join9 """
            SELECT '' AS "xxx", *
            FROM ${TBname1}
            RIGHT JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """
    qt_join10 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} FULL OUTER JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join11 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} FULL JOIN ${TBname2} USING (i)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join12 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} LEFT JOIN ${TBname2} USING (i)
            WHERE (k = 1)
            ORDER BY 1,2,3,4,5,6;
        """

    qt_join13 """
            SELECT '' AS "xxx", *
            FROM ${TBname1} LEFT JOIN ${TBname2} USING (i)
            WHERE (${TBname1}.i = 1)
            ORDER BY 1,2,3,4,5,6;
        """

    sql "DROP TABLE IF EXISTS ${TBname1};"
    sql "DROP TABLE IF EXISTS ${TBname2};"
    sql "DROP DATABASE IF EXISTS ${DBname};"
}
