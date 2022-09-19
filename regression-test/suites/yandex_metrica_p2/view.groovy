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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("view") {
    sql """
        DROP VIEW IF EXISTS `yandex_view`
    """

    sql """
        CREATE VIEW yandex_view AS SELECT CounterID, count() AS c FROM hits GROUP BY CounterID
    """

    qt_view_1 """
        SELECT count() FROM yandex_view
    """

    qt_view_2 """
        SELECT c, count() FROM yandex_view GROUP BY c ORDER BY count() DESC LIMIT 10
    """

    qt_view_3 """
        SELECT * FROM yandex_view ORDER BY c DESC LIMIT 10
    """

    sql """
        DROP VIEW IF EXISTS `yandex_view`
    """
}
