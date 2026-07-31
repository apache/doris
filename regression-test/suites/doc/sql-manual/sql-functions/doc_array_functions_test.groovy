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

suite("doc_array_functions_test") {
    sql ''' DROP TABLE IF EXISTS doc_array_concat_test; '''
    sql '''
        CREATE TABLE doc_array_concat_test (
            id INT,
            int_array1 ARRAY<INT>,
            int_array2 ARRAY<INT>,
            string_array1 ARRAY<STRING>,
            string_array2 ARRAY<STRING>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    '''
    sql '''
        INSERT INTO doc_array_concat_test VALUES
        (1, [1, 2, 3], [4, 5, 6], ['a', 'b'], ['c', 'd']),
        (2, [10, 20], [30, 40], [], ['x', 'y']),
        (3, NULL, [100, 200], NULL, ['z']),
        (4, [], [], [], []),
        (5, [1, null, 3], [null, 5, 6], ['a', null, 'c'], ['d', 'e']);
    '''

    qt_array_concat_literals '''
        SELECT array_concat([1, 2], [7, 8], [5, 6]);
    '''
    testFoldConst('''
        SELECT array_concat([1, 2], [7, 8], [5, 6]);
    ''')

    qt_array_concat_string_columns '''
        SELECT array_concat(string_array1, string_array2) FROM doc_array_concat_test WHERE id = 1;
    '''

    qt_array_concat_empty '''
        SELECT array_concat([], []);
    '''
    testFoldConst('''
        SELECT array_concat([], []);
    ''')

    qt_array_concat_null '''
        SELECT array_concat(int_array1, int_array2) FROM doc_array_concat_test WHERE id = 3;
    '''

    qt_array_concat_null_elements '''
        SELECT array_concat(int_array1, int_array2) FROM doc_array_concat_test WHERE id = 5;
    '''

    qt_array_concat_type_compatible '''
        SELECT array_concat(int_array1, string_array1) FROM doc_array_concat_test WHERE id = 1;
    '''

    qt_array_concat_nested_array '''
        SELECT array_concat([[1,2],[3,4]], [[5,6],[7,8]]);
    '''
    testFoldConst('''
        SELECT array_concat([[1,2],[3,4]], [[5,6],[7,8]]);
    ''')

    test {
        sql ''' SELECT array_concat([[1,2]], [{'k':1}]); '''
        exception "can not cast"
    }

    qt_array_concat_map '''
        SELECT array_concat([{'k':1}], [{'k':2}]);
    '''
    testFoldConst('''
        SELECT array_concat([{'k':1}], [{'k':2}]);
    ''')

    qt_array_concat_struct '''
        SELECT array_concat(array(named_struct('name','Alice','age',20)), array(named_struct('name','Bob','age',30)));
    '''
    testFoldConst('''
        SELECT array_concat(array(named_struct('name','Alice','age',20)), array(named_struct('name','Bob','age',30)));
    ''')

    test {
        sql ''' SELECT array_concat(array(named_struct('name','Alice','age',20)), array(named_struct('id',1,'score',95.5,'age',10))); '''
        exception "can not cast"
    }

    test {
        sql ''' SELECT array_concat(); '''
        exception "array_concat"
    }

    test {
        sql ''' SELECT array_concat('not_an_array'); '''
        exception "array_concat"
    }

    sql ''' DROP TABLE IF EXISTS doc_array_cum_sum_test; '''
    sql '''
        CREATE TABLE doc_array_cum_sum_test (
            id INT,
            int_array ARRAY<INT>,
            double_array ARRAY<DOUBLE>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    '''
    sql '''
        INSERT INTO doc_array_cum_sum_test VALUES
        (1, [1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5]),
        (2, [10, 20, 30], [10.5, 20.5, 30.5]),
        (3, [], []),
        (4, NULL, NULL);
    '''

    qt_array_cum_sum_int '''
        SELECT array_cum_sum(int_array) FROM doc_array_cum_sum_test WHERE id = 1;
    '''

    qt_array_cum_sum_double '''
        SELECT array_cum_sum(double_array) FROM doc_array_cum_sum_test WHERE id = 1;
    '''

    qt_array_cum_sum_mixed '''
        SELECT array_cum_sum(['a', 1, 'b', 2, 'c', 3]);
    '''
    testFoldConst('''
        SELECT array_cum_sum(['a', 1, 'b', 2, 'c', 3]);
    ''')

    qt_array_cum_sum_empty '''
        SELECT array_cum_sum(int_array) FROM doc_array_cum_sum_test WHERE id = 3;
    '''

    qt_array_cum_sum_null '''
        SELECT array_cum_sum(int_array) FROM doc_array_cum_sum_test WHERE id = 4;
    '''

    qt_array_cum_sum_single '''
        SELECT array_cum_sum([42]);
    '''
    testFoldConst('''
        SELECT array_cum_sum([42]);
    ''')

    qt_array_cum_sum_null_elements '''
        SELECT array_cum_sum([null, 1, null, 3, null, 5]);
    '''
    testFoldConst('''
        SELECT array_cum_sum([null, 1, null, 3, null, 5]);
    ''')

    test {
        sql ''' SELECT array_cum_sum([[1,2,3]]); '''
        exception "array_cum_sum"
    }

    test {
        sql ''' SELECT array_cum_sum([{'k':1},{'k':2}]); '''
        exception "array_cum_sum"
    }

    test {
        sql ''' SELECT array_cum_sum(array(named_struct('name','Alice','age',20),named_struct('name','Bob','age',30))); '''
        exception "array_cum_sum"
    }

    test {
        sql ''' SELECT array_cum_sum([1,2,3],[4,5,6]); '''
        exception "array_cum_sum"
    }

    test {
        sql ''' SELECT array_cum_sum('not_an_array'); '''
        exception "array_cum_sum"
    }

    qt_array_popback_string '''
        SELECT array_popback(['apple', 'banana', 'cherry', 'date']);
    '''
    testFoldConst('''
        SELECT array_popback(['apple', 'banana', 'cherry', 'date']);
    ''')

    qt_array_popback_null_elements '''
        SELECT array_popback([1, null, 3, null, 5]);
    '''
    testFoldConst('''
        SELECT array_popback([1, null, 3, null, 5]);
    ''')

    qt_array_popback_single '''
        SELECT array_popback([42]);
    '''
    testFoldConst('''
        SELECT array_popback([42]);
    ''')

    qt_array_popback_empty '''
        SELECT array_popback([]);
    '''
    testFoldConst('''
        SELECT array_popback([]);
    ''')

    qt_array_popback_null '''
        SELECT array_popback(NULL);
    '''
    testFoldConst('''
        SELECT array_popback(NULL);
    ''')

    qt_array_popback_ipv4 '''
        SELECT array_popback(CAST(['192.168.1.1', '192.168.1.2', '192.168.1.3'] AS ARRAY<IPV4>));
    '''
    testFoldConst('''
        SELECT array_popback(CAST(['192.168.1.1', '192.168.1.2', '192.168.1.3'] AS ARRAY<IPV4>));
    ''')

    qt_array_popback_nested '''
        SELECT array_popback([[1, 2], [3, 4], [5, 6]]);
    '''
    testFoldConst('''
        SELECT array_popback([[1, 2], [3, 4], [5, 6]]);
    ''')

    qt_array_popback_map '''
        SELECT array_popback([{'name':'Alice','age':20}, {'name':'Bob','age':30}, {'name':'Charlie','age':40}]);
    '''
    testFoldConst('''
        SELECT array_popback([{'name':'Alice','age':20}, {'name':'Bob','age':30}, {'name':'Charlie','age':40}]);
    ''')

    qt_array_popback_struct '''
        SELECT array_popback(array(named_struct('name','Alice','age',20), named_struct('name','Bob','age',30), named_struct('name','Charlie','age',40)));
    '''
    testFoldConst('''
        SELECT array_popback(array(named_struct('name','Alice','age',20), named_struct('name','Bob','age',30), named_struct('name','Charlie','age',40)));
    ''')

    qt_array_popfront_string '''
        SELECT array_popfront(['apple', 'banana', 'cherry', 'date']);
    '''
    testFoldConst('''
        SELECT array_popfront(['apple', 'banana', 'cherry', 'date']);
    ''')

    qt_array_popfront_null_elements '''
        SELECT array_popfront([1, null, 3, null, 5]);
    '''
    testFoldConst('''
        SELECT array_popfront([1, null, 3, null, 5]);
    ''')

    qt_array_popfront_single '''
        SELECT array_popfront([42]);
    '''
    testFoldConst('''
        SELECT array_popfront([42]);
    ''')

    qt_array_popfront_empty '''
        SELECT array_popfront([]);
    '''
    testFoldConst('''
        SELECT array_popfront([]);
    ''')

    qt_array_popfront_null '''
        SELECT array_popfront(NULL);
    '''
    testFoldConst('''
        SELECT array_popfront(NULL);
    ''')

    qt_array_popfront_ipv4 '''
        SELECT array_popfront(CAST(['192.168.1.1', '192.168.1.2', '192.168.1.3'] AS ARRAY<IPV4>));
    '''
    testFoldConst('''
        SELECT array_popfront(CAST(['192.168.1.1', '192.168.1.2', '192.168.1.3'] AS ARRAY<IPV4>));
    ''')

    qt_array_popfront_nested '''
        SELECT array_popfront([[1, 2], [3, 4], [5, 6]]);
    '''
    testFoldConst('''
        SELECT array_popfront([[1, 2], [3, 4], [5, 6]]);
    ''')

    qt_array_popfront_map '''
        SELECT array_popfront([{'name':'Alice','age':20}, {'name':'Bob','age':30}, {'name':'Charlie','age':40}]);
    '''
    testFoldConst('''
        SELECT array_popfront([{'name':'Alice','age':20}, {'name':'Bob','age':30}, {'name':'Charlie','age':40}]);
    ''')

    qt_array_popfront_struct '''
        SELECT array_popfront(array(named_struct('name','Alice','age',20), named_struct('name','Bob','age',30), named_struct('name','Charlie','age',40)));
    '''
    testFoldConst('''
        SELECT array_popfront(array(named_struct('name','Alice','age',20), named_struct('name','Bob','age',30), named_struct('name','Charlie','age',40)));
    ''')

    sql ''' DROP TABLE IF EXISTS doc_array_test2; '''
    sql '''
        CREATE TABLE doc_array_test2 (
            id INT,
            c_array1 ARRAY<INT>,
            c_array2 ARRAY<INT>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
          "replication_num" = "1"
        );
    '''
    sql '''
        INSERT INTO doc_array_test2 VALUES
        (1, [1, 2, 3, 4, 5], [10, 20, -40, 80, -100]),
        (2, [6, 7, 8], [10, 12, 13]),
        (3, [1], [-100]),
        (4, NULL, NULL);
    '''

    qt_array_exists_lambda_const '''
        SELECT *, array_exists(x -> x > 1, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    '''
    testFoldConst('''
        SELECT *, array_exists(x -> x > 1, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    ''')

    qt_array_exists_even '''
        SELECT c_array1, c_array2, array_exists(x -> x % 2 = 0, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    '''
    testFoldConst('''
        SELECT c_array1, c_array2, array_exists(x -> x % 2 = 0, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    ''')

    qt_array_exists_abs '''
        SELECT c_array1, c_array2, array_exists(x -> abs(x) - 1, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    '''
    testFoldConst('''
        SELECT c_array1, c_array2, array_exists(x -> abs(x) - 1, [1,2,3]) FROM doc_array_test2 ORDER BY id;
    ''')

    qt_array_exists_columns '''
        SELECT c_array1, c_array2, array_exists((x,y) -> x > y, c_array1, c_array2) FROM doc_array_test2 ORDER BY id;
    '''

    qt_array_exists_column_only '''
        SELECT *, array_exists(c_array1) FROM doc_array_test2 ORDER BY id;
    '''

    qt_array_split_basic '''
        SELECT array_split([1,2,3,4,5], [1,0,1,0,0]);
    '''
    testFoldConst('''
        SELECT array_split([1,2,3,4,5], [1,0,1,0,0]);
    ''')

    qt_array_split_lambda '''
        SELECT array_split((x,y) -> y, [1,2,3,4,5], [1,0,0,0,0]);
    '''
    testFoldConst('''
        SELECT array_split((x,y) -> y, [1,2,3,4,5], [1,0,0,0,0]);
    ''')

    qt_array_split_lambda_expression '''
        SELECT array_split((x,y) -> (y+1), ['a', 'b', 'c', 'd'], [-1, -1, 0, -1]);
    '''
    testFoldConst('''
        SELECT array_split((x,y) -> (y+1), ['a', 'b', 'c', 'd'], [-1, -1, 0, -1]);
    ''')

    qt_array_split_date '''
        SELECT array_split(x -> (year(x) > 2013), ["2020-12-12", "2013-12-12", "2015-12-12", NULL]);
    '''
    testFoldConst('''
        SELECT array_split(x -> (year(x) > 2013), ["2020-12-12", "2013-12-12", "2015-12-12", NULL]);
    ''')
}
