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

// Regression test for: length() / char_length() on dict-encoded (low-cardinality)
// varchar columns must not throw NOT_IMPLEMENTED_ERROR when the "only_read_offsets"
// optimisation is active (i.e. when the storage layer resolves dict codes to string
// lengths without materialising actual character data).
suite("test_length_dict_encoded") {
    sql "DROP TABLE IF EXISTS test_length_dict_varchar"
    sql """
        CREATE TABLE test_length_dict_varchar (
          col_int_undef_signed           int          NULL,
          col_int_undef_signed_not_null  int          NOT NULL,
          col_date_undef_signed          date         NULL,
          col_date_undef_signed_not_null date         NOT NULL,
          col_varchar_5__undef_signed    varchar(5)   NULL,
          col_varchar_5__undef_signed_not_null varchar(5) NOT NULL,
          pk                             int          NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(col_int_undef_signed)
        PARTITION BY RANGE(col_int_undef_signed) (
          PARTITION p0   VALUES [('-2147483648'), ('4')),
          PARTITION p1   VALUES [('4'),  ('6')),
          PARTITION p2   VALUES [('6'),  ('7')),
          PARTITION p3   VALUES [('7'),  ('8')),
          PARTITION p4   VALUES [('8'),  ('10')),
          PARTITION p5   VALUES [('10'), ('83647')),
          PARTITION p100 VALUES [('83647'), ('2147483647'))
        )
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ('replication_allocation' = 'tag.location.default: 1')
    """

    sql """
        INSERT INTO test_length_dict_varchar VALUES
          (6,     5,        '2023-12-13', '2023-12-11', 'o', 'i', 30),
          (6,     6,        NULL,         '2023-12-18', 'w', 'l', 24),
          (8,     -8278102, '2023-12-13', '2023-12-11', 'x', 'c', 28),
          (15971, 8,        NULL,         '2015-06-11', 'h', 'r', 41),
          (6,     5,        '2023-12-11', '2023-12-17', 'd', 'q',  5)
    """

    // length() on nullable dict-encoded varchar filtered by IS NOT NULL predicate
    order_qt_length_nullable_not_null """
        SELECT length(col_varchar_5__undef_signed)
        FROM   test_length_dict_varchar
        WHERE  col_varchar_5__undef_signed IS NOT NULL
    """

    // length() on NOT NULL dict-encoded varchar (no predicate needed)
    order_qt_length_not_null_col """
        SELECT length(col_varchar_5__undef_signed_not_null)
        FROM   test_length_dict_varchar
    """

    // length() returns NULL for NULL varchar values
    order_qt_length_with_nulls """
        SELECT length(col_varchar_5__undef_signed), pk
        FROM   test_length_dict_varchar
        ORDER BY pk
    """

    // char_length() is a synonym and must work as well
    order_qt_char_length_nullable_not_null """
        SELECT char_length(col_varchar_5__undef_signed)
        FROM   test_length_dict_varchar
        WHERE  col_varchar_5__undef_signed IS NOT NULL
    """
}
