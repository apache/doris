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

suite("test_alias_function") {

    sql """DROP FUNCTION IF EXISTS mesh_udf_test1(INT,INT)"""
    sql """CREATE ALIAS FUNCTION mesh_udf_test1(INT,INT) WITH PARAMETER(n,d) AS ROUND(1+floor(n/d));"""
    qt_sql1 """select mesh_udf_test1(1,2);"""

    sql """DROP FUNCTION IF EXISTS mesh_udf_test2(INT,INT)"""
    sql """CREATE ALIAS FUNCTION mesh_udf_test2(INT,INT) WITH PARAMETER(n,d) AS add(1,floor(divide(n,d)))"""
    qt_sql1 """select mesh_udf_test2(1,2);"""


    sql """DROP FUNCTION IF EXISTS userlevel(bigint)"""
    test {
          sql """create GLOBAL ALIAS FUNCTION userlevel(bigint) with PARAMETER(level_score) as (CASE WHEN level_score < 0 THEN 0 WHEN level_score < 1000 THEN 1 WHEN level_score < 5000 THEN 2 WHEN level_score < 10000 THEN 3 WHEN level_score < 407160000 THEN 29 ELSE 30 END);"""
          exception "Not supported expr type"
    }


    sql """DROP FUNCTION IF EXISTS userlevel(bigint)"""
    test {
          sql """CREATE GLOBAL ALIAS FUNCTION userlevel(bigint) WITH PARAMETER(level_score) AS
            IF(level_score < 0, 0,

                IF(level_score BETWEEN 0 AND 999, 1,

                    IF(level_score BETWEEN 1000 AND 4999, 2,

                        IF(level_score BETWEEN 5000 AND 9999, 3,

                            IF(level_score BETWEEN 10000 AND 15999, 4,

                                IF(level_score BETWEEN 16000 AND 23499, 5,

                                    IF(level_score BETWEEN 23500 AND 33999, 6,

                                        IF(level_score BETWEEN 34000 AND 47499, 7,

                                            IF(level_score BETWEEN 47500 AND 63999, 8,

                                                IF(level_score BETWEEN 64000 AND 83499, 9,

                                                    IF(level_score BETWEEN 83500 AND 113499, 10,

                                                        IF(level_score BETWEEN 113500 AND 155499, 11,

                                                            IF(level_score BETWEEN 155500 AND 209499, 12,

                                                                IF(level_score BETWEEN 209500 AND 275499, 13,

                                                                    IF(level_score BETWEEN 275500 AND 353499, 14,

                                                                        IF(level_score BETWEEN 353500 AND 533499, 15,

                                                                            IF(level_score BETWEEN 533500 AND 785499, 16,

                                                                                IF(level_score BETWEEN 785500 AND 1109499, 17,

                                                                                    IF(level_score BETWEEN 1109500 AND 1505499, 18,

                                                                                        IF(level_score BETWEEN 1505500 AND 1973499, 19,

                                                                                            IF(level_score BETWEEN 1973500 AND 2333499, 20,

                                                                                                IF(level_score BETWEEN 2333500 AND 2837499, 21,

                                                                                                    IF(level_score BETWEEN 2837500 AND 3485499, 22,

                                                                                                        IF(level_score BETWEEN 3485500 AND 4277499, 23,

                                                                                                            IF(level_score BETWEEN 4277500 AND 5213499, 24,

                                                                                                                IF(level_score BETWEEN 5213500 AND 25447499, 25,

                                                                                                                    IF(level_score BETWEEN 25447500 AND 50894999, 26,

                                                                                                                        IF(level_score BETWEEN 50895000 AND 101789999, 27,

                                                                                                                            IF(level_score BETWEEN 101790000 AND 203579999, 28,

                                                                                                                                IF(level_score BETWEEN 203580000 AND 407159999, 29,

                                                                                                                                    IF(level_score >= 407160000, 30, 0)

                                                                                                                                )

                                                                                                                            )

                                                                                                                        )

                                                                                                                    )

                                                                                                                )

                                                                                                            )

                                                                                                        )

                                                                                                    )

                                                                                                )

                                                                                            )

                                                                                        )

                                                                                    )

                                                                                )

                                                                            )

                                                                        )

                                                                    )

                                                                )

                                                            )

                                                        )

                                                    )

                                                )

                                            )

                                        )

                                    )

                                )

                            )

                        )

                    )

                )

            )
        """
    }
}
