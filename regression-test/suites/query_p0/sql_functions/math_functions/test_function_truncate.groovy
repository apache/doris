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

suite("test_function_truncate") {
    // NOTICE: This single const argument test should never cause BE crash,
    // like branch2.0's behavior, so we added it to check.
    qt_sql """SELECT truncate(10.12345), truncate(cast(10.12345 as decimal(7, 5)));"""

    qt_sql """
        SELECT number, truncate(123.345 , 1) FROM numbers("number"="10");
    """
    qt_sql """
        SELECT number, truncate(123.123, -1) FROM numbers("number"="10");
    """
    qt_sql """
        SELECT number, truncate(123.123, 0) FROM numbers("number"="10");
    """

    // const_const, result scale should be 10
    qt_sql """
        SELECT truncate(cast(0 as Decimal(9,8)), 10);
    """  

    // const_const, result scale should be 1
    qt_sql """
        SELECT number, truncate(cast(0 as Decimal(9,4)), 1) FROM numbers("number"="5")
    """

    sql """DROP TABLE IF EXISTS test_function_truncate;"""
    sql """DROP TABLE IF EXISTS test_function_truncate_dec128;"""
    sql """
        CREATE TABLE test_function_truncate (
            rid int, flo float, dou double,
            dec90 decimal(9, 0), dec91 decimal(9, 1), dec99 decimal(9, 9),
            dec100 decimal(10,0), dec109 decimal(10,9), dec1010 decimal(10,10),
            number int DEFAULT 1)
        DISTRIBUTED BY HASH(rid)
        PROPERTIES("replication_num" = "1" );
        """

    sql """
        INSERT INTO test_function_truncate
        VALUES
            (1, 12345.123, 123456789.123456789,
             123456789, 12345678.1, 0.123456789,
             123456789.1, 1.123456789, 0.123456789, 1);
    """
    sql """
        INSERT INTO test_function_truncate
            VALUES
        (2, 12345.123, 123456789.123456789,
             123456789, 12345678.1, 0.123456789,
             123456789.1, 1.123456789, 0.123456789, 1);
    """
    sql """
        INSERT INTO test_function_truncate
            VALUES
        (3, 12345.123, 123456789.123456789,
             123456789, 12345678.1, 0.123456789,
             123456789.1, 1.123456789, 0.123456789, 1);
    """
    sql """
        INSERT INTO test_function_truncate
            VALUES
        (4, 0, 0, 0, 0.0, 0, 0, 0, 0, 1);
    """
    qt_vec_const0 """
        SELECT rid, truncate(flo, 0), truncate(dou, 0) FROM test_function_truncate order by rid;
    """
    qt_vec_const0 """
        SELECT rid, truncate(flo, 1), truncate(dou, 1) FROM test_function_truncate order by rid;
    """
    qt_vec_const0 """
        SELECT rid, truncate(flo, -1), truncate(dou, -1) FROM test_function_truncate order by rid;
    """
    qt_vec_const1 """
        SELECT rid, dec90, truncate(dec90, 0), dec91, truncate(dec91, 0), dec99, truncate(dec99, 0) FROM test_function_truncate order by rid
    """
    qt_vec_const2 """
        SELECT rid, dec100, truncate(dec100, 0), dec109, truncate(dec109, 0), dec1010, truncate(dec1010, 0) FROM test_function_truncate order by rid
    """

    

    qt_const_vec1 """
        SELECT 123456789.123456789, number, truncate(123456789.123456789, number) from test_function_truncate;
    """
    qt_const_vec2 """
        SELECT 123456789.123456789, -number, truncate(123456789.123456789, -number) from test_function_truncate;
    """
    qt_vec_vec0 """
        SELECT rid,number, truncate(flo, number), truncate(dou, number) FROM test_function_truncate order by rid;
    """

    sql """
        CREATE TABLE test_function_truncate_dec128 (
            rid int, dec190 decimal(19,0), dec199 decimal(19,9), dec1919 decimal(19,19),
                     dec380 decimal(38,0), dec3819 decimal(38,19), dec3838 decimal(38,38),
                     number int DEFAULT 1
        )
        DISTRIBUTED BY HASH(rid)
        PROPERTIES("replication_num" = "1" );
    """
    sql """
        INSERT INTO test_function_truncate_dec128
            VALUES
        (1, 1234567891234567891.0, 1234567891.123456789, 0.1234567891234567891,
            12345678912345678912345678912345678912.0, 
            1234567891234567891.1234567891234567891,
            0.12345678912345678912345678912345678912345678912345678912345678912345678912, 1);
    """
    qt_truncate_dec128 """
        SELECT rid, dec190, truncate(dec190, 0), dec199, truncate(dec199, 0), dec1919, truncate(dec1919, 0)
            FROM test_function_truncate_dec128 order by rid
    """

    qt_truncate_dec128 """
        SELECT rid, dec190, truncate(dec190, number), dec199, truncate(dec199, number), dec1919, truncate(dec1919, number)
            FROM test_function_truncate_dec128 order by rid
    """

}