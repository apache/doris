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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("set_with_null") {
    sql " drop table if exists d_table;"
    sql """
    create table d_table (
        k1 int null
    )
    duplicate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
   sql "insert into d_table values (null);"

   qt_test """
    (
        select k1
        from d_table
    )
    intersect
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    except
    (
        select null
    )
   """

    sql " drop table if exists d_table;"
    sql """
    create table d_table (
        k1 int null
    )
    duplicate key (k1)
    distributed BY hash(k1) buckets 3
    properties("replication_num" = "1");
    """
   sql "insert into d_table values (null),(1);"

    qt_test """
    (
        select k1
        from d_table
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
    intersect
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
   """

    qt_test """
    (
        select k1
        from d_table
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
    except
    (
        select null
    )
   """
}